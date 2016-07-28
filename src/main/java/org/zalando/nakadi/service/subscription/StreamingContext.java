package org.zalando.nakadi.service.subscription;

import org.zalando.nakadi.exceptions.ExceptionWrapper;
import org.zalando.nakadi.service.subscription.model.Partition;
import org.zalando.nakadi.service.subscription.model.Session;
import org.zalando.nakadi.service.subscription.state.CleanupState;
import org.zalando.nakadi.service.subscription.state.DummyState;
import org.zalando.nakadi.service.subscription.state.StartingState;
import org.zalando.nakadi.service.subscription.state.State;
import org.zalando.nakadi.service.subscription.zk.ZKSubscription;
import org.zalando.nakadi.service.subscription.zk.ZkSubscriptionClient;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamingContext implements SubscriptionStreamer {
    private final StreamParameters parameters;
    private final Session session;
    private final ZkSubscriptionClient zkClient;
    private final KafkaClient kafkaClient;
    private final SubscriptionOutput out;
    private final long kafkaPollTimeout;
    private final AtomicBoolean connectionReady;

    private final ScheduledExecutorService timer;
    private final BlockingQueue<Runnable> taskQueue = new LinkedBlockingQueue<>();
    private final BiFunction<Session[], Partition[], Partition[]> rebalancer;

    private final String loggingPath;

    private State currentState = new DummyState();
    private ZKSubscription clientListChanges;

    public static final State DEAD_STATE = new DummyState();

    private final Logger log;

    StreamingContext(
            final SubscriptionOutput out,
            final StreamParameters parameters,
            final Session session,
            final ScheduledExecutorService timer,
            final ZkSubscriptionClient zkClient,
            final KafkaClient kafkaClient,
            final BiFunction<Session[], Partition[], Partition[]> rebalancer,
            final long kafkaPollTimeout,
            final String loggingPath,
            final AtomicBoolean connectionReady) {
        this.out = out;
        this.parameters = parameters;
        this.session = session;
        this.rebalancer = rebalancer;
        this.timer = timer;
        this.zkClient = zkClient;
        this.kafkaClient = kafkaClient;
        this.kafkaPollTimeout = kafkaPollTimeout;
        this.loggingPath = loggingPath + ".stream";
        this.log = LoggerFactory.getLogger(loggingPath);
        this.connectionReady = connectionReady;
    }

    public StreamParameters getParameters() {
        return parameters;
    }

    public ZkSubscriptionClient getZkClient() {
        return zkClient;
    }

    public String getSessionId() {
        return session.getId();
    }

    public KafkaClient getKafkaClient() {
        return kafkaClient;
    }

    public SubscriptionOutput getOut() {
        return out;
    }

    public long getKafkaPollTimeout() {
        return kafkaPollTimeout;
    }

    @Override
    public void stream() throws InterruptedException {
        streamInternal(new StartingState());
    }

    void streamInternal(final State firstState) throws InterruptedException {
        // Add first task - switch to starting state.
        switchState(firstState);

        while (currentState != DEAD_STATE) {
            // Wait forever
            final Runnable task = taskQueue.poll(1, TimeUnit.HOURS);
            try {
                if (task != null) {
                    task.run();
                }
            } catch (final ExceptionWrapper ex) {
                log.error("Failed to process task " + task + ", will rethrow original error", ex);
                switchState(new CleanupState(ex.getWrapped()));
            } catch (final RuntimeException ex) {
                log.error("Failed to process task " + task + ", code carefully!", ex);
                switchState(new CleanupState(ex));
            }
        }
    }

    public void switchState(final State newState) {
        this.addTask(() -> {
            log.info("Switching state from " + currentState.getClass().getSimpleName());
            currentState.onExit();

            currentState = newState;

            log.info("Switching state to " + currentState.getClass().getSimpleName());
            currentState.setContext(this, loggingPath);
            currentState.onEnter();
        });
    }

    public void registerSession() {
        log.info("Registering session {}", session);
        // Install rebalance hook on client list change.
        clientListChanges = zkClient.subscribeForSessionListChanges(() -> addTask(this::rebalance));
        zkClient.registerSession(session);
    }

    public void unregisterSession() {
        log.info("Unregistering session {}", session);
        if (null != clientListChanges) {
            try {
                clientListChanges.cancel();
            } finally {
                this.clientListChanges = null;
                zkClient.unregisterSession(session);
            }
        }
    }

    public boolean isInState(final State state) {
        return currentState == state;
    }

    public void addTask(final Runnable task) {
        taskQueue.offer(task);
    }

    public void scheduleTask(final Runnable task, final long timeout, final TimeUnit unit) {
        timer.schedule(() -> this.addTask(task), timeout, unit);
    }

    public boolean isConnectionReady() {
        return connectionReady.get();
    }

    private void rebalance() {
        if (null != clientListChanges) {
            clientListChanges.refresh();
            zkClient.runLocked(() -> {
                final Partition[] changeset = rebalancer.apply(zkClient.listSessions(), zkClient.listPartitions());
                if (changeset.length > 0) {
                    Stream.of(changeset).forEach(zkClient::updatePartitionConfiguration);
                    zkClient.incrementTopology();
                }
            });
        }
    }

}
