package de.zalando.aruha.nakadi.service.subscription;

import de.zalando.aruha.nakadi.service.subscription.model.Partition;
import de.zalando.aruha.nakadi.service.subscription.model.Session;
import de.zalando.aruha.nakadi.service.subscription.state.CleanupState;
import de.zalando.aruha.nakadi.service.subscription.state.StartingState;
import de.zalando.aruha.nakadi.service.subscription.state.State;
import de.zalando.aruha.nakadi.service.subscription.state.StreamCreatedState;
import de.zalando.aruha.nakadi.service.subscription.zk.ZkSubscriptionClient;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamingContext implements SubscriptionStreamer {
    public final StreamParameters parameters;
    public final Session session;
    public final ZkSubscriptionClient zkClient;
    public final KafkaClient kafkaClient;
    public final SubscriptionOutput out;

    private final ScheduledExecutorService timer;
    private final BlockingQueue<Runnable> taskQueue = new LinkedBlockingQueue<>();
    private final BiFunction<Session[], Partition[], Partition[]> rebalancer;

    private State currentState;
    private ZkSubscriptionClient.ZKSubscription clientListChanges;

    private static final Logger LOG = LoggerFactory.getLogger(StreamingContext.class);

    StreamingContext(
            final SubscriptionOutput out,
            final StreamParameters parameters,
            final Session session,
            final ScheduledExecutorService timer,
            final ZkSubscriptionClient zkClient,
            final KafkaClient kafkaClient,
            final BiFunction<Session[], Partition[], Partition[]> rebalancer) {
        this.out = out;
        this.parameters = parameters;
        this.session = session;
        this.rebalancer = rebalancer;
        this.timer = timer;
        this.zkClient = zkClient;
        this.currentState = new StreamCreatedState(this);
        this.kafkaClient = kafkaClient;
    }

    @Override
    public void stream() throws InterruptedException {
        // Because all the work is processed inside one thread, there is no need in
        // additional lock.
        currentState = new StreamCreatedState(this);

        // Add first task - switch to starting state.
        switchState(new StartingState(this));

        while (null != currentState) {
            final Runnable task = taskQueue.poll(1, TimeUnit.MINUTES);
            try {
                task.run();
            } catch (final RuntimeException ex) {
                LOG.error("Failed to process task " + task + ", code carefully!", ex);
                switchState(new CleanupState(this, ex));
            }
        }
    }

    public void switchState(final State newState) {
        this.addTask(() -> {
            if (currentState != null) {
                LOG.info("Switching state from " + currentState.getClass().getSimpleName());
                currentState.onExit();
            } else {
                LOG.info("Connection died, no new state switches available");
                return;
            }
            currentState = newState;
            if (null != currentState) {
                LOG.info("Switching state to " + currentState.getClass().getSimpleName());
                currentState.onEnter();
            } else {
                LOG.info("No next state found, dying");
            }
        });
    }

    public boolean registerSession() {
        LOG.info("Registering session " + session);
        final boolean sessionRegistered = zkClient.registerSession(session);
        if (sessionRegistered) {
            // Session registered successfully.

            // Install rebalance hook on client list change.
            clientListChanges = zkClient.subscribeForSessionListChanges(() -> addTask(this::rebalance));

            // Invoke rebalance directly
            addTask(this::rebalance);
        }
        return sessionRegistered;
    }

    public void unregisterSession() {
        LOG.info("Unregistering session " + session);
        if (null != clientListChanges) {
            try {
                clientListChanges.cancel();
            } finally {
                this.clientListChanges = null;
                zkClient.unregisterSession(session);
            }
        }
    }

    public boolean isInState(State state) {
        return currentState == state;
    }

    public void addTask(Runnable task) {
        taskQueue.offer(task);
    }

    public void scheduleTask(final Runnable task, final long timeout, final TimeUnit unit) {
        timer.schedule(() -> this.addTask(task), timeout, unit);
    }

    private void rebalance() {
        zkClient.lock(() -> {
            final Partition[] changeset = rebalancer.apply(zkClient.listSessions(), zkClient.listPartitions());
            if (changeset != null && changeset.length > 0) {
                Stream.of(changeset).forEach(zkClient::updatePartitionConfiguration);
                zkClient.incrementTopology();
            }
        });
    }

    public void onZkException(Exception e) {
        LOG.error("ZK exception occurred, switching to CleanupState", e);
        switchState(new CleanupState(this, e));
        if (e instanceof InterruptedException) {
            Thread.currentThread().interrupt();
        }
    }

    public void onKafkaException(Exception e) {
        LOG.error("Kafka exception occured, switching to CleanupState", e);
        switchState(new CleanupState(this, e));
        if (e instanceof InterruptedException) {
            Thread.currentThread().interrupt();
        }
    }

}
