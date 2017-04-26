package org.zalando.nakadi.service.subscription;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.exceptions.NakadiRuntimeException;
import org.zalando.nakadi.service.BlacklistService;
import org.zalando.nakadi.service.CursorConverter;
import org.zalando.nakadi.service.CursorTokenService;
import org.zalando.nakadi.service.subscription.model.Partition;
import org.zalando.nakadi.service.subscription.model.Session;
import org.zalando.nakadi.service.subscription.state.CleanupState;
import org.zalando.nakadi.service.subscription.state.DummyState;
import org.zalando.nakadi.service.subscription.state.StartingState;
import org.zalando.nakadi.service.subscription.state.State;
import org.zalando.nakadi.service.subscription.zk.ZKSubscription;
import org.zalando.nakadi.service.subscription.zk.ZkSubscriptionClient;
import org.zalando.nakadi.service.timeline.TimelineService;

public class StreamingContext implements SubscriptionStreamer {

    public static final State DEAD_STATE = new DummyState();

    private final StreamParameters parameters;
    private final Session session;
    private final ZkSubscriptionClient zkClient;
    private final SubscriptionOutput out;
    private final long kafkaPollTimeout;
    private final AtomicBoolean connectionReady;
    private final TimelineService timelineService;
    private final CursorTokenService cursorTokenService;
    private final ObjectMapper objectMapper;
    private final BlacklistService blacklistService;
    private final ScheduledExecutorService timer;
    private final BlockingQueue<Runnable> taskQueue = new LinkedBlockingQueue<>();
    private final BiFunction<Session[], Partition[], Partition[]> rebalancer;
    private final String loggingPath;
    private final CursorConverter cursorConverter;
    private final Subscription subscription;
    private final MetricRegistry metricRegistry;
    private State currentState = new DummyState();
    private ZKSubscription clientListChanges;

    private final Logger log;

    private StreamingContext(final Builder builder) {
        this.out = builder.out;
        this.parameters = builder.parameters;
        this.session = builder.session;
        this.rebalancer = builder.rebalancer;
        this.timer = builder.timer;
        this.zkClient = builder.zkClient;
        this.kafkaPollTimeout = builder.kafkaPollTimeout;
        this.loggingPath = builder.loggingPath + ".stream";
        this.log = LoggerFactory.getLogger(builder.loggingPath);
        this.connectionReady = builder.connectionReady;
        this.timelineService = builder.timelineService;
        this.cursorTokenService = builder.cursorTokenService;
        this.objectMapper = builder.objectMapper;
        this.blacklistService = builder.blacklistService;
        this.cursorConverter = builder.cursorConverter;
        this.subscription = builder.subscription;
        this.metricRegistry = builder.metricRegistry;
    }

    public TimelineService getTimelineService() {
        return timelineService;
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

    public SubscriptionOutput getOut() {
        return out;
    }

    public long getKafkaPollTimeout() {
        return kafkaPollTimeout;
    }

    public CursorConverter getCursorConverter() {
        return cursorConverter;
    }

    public Subscription getSubscription() {
        return subscription;
    }

    public MetricRegistry getMetricRegistry() {
        return metricRegistry;
    }

    @Override
    public void stream() throws InterruptedException {
        // bugfix ARUHA-485
        Runtime.getRuntime().addShutdownHook(new Thread(() -> onNodeShutdown()));
        streamInternal(new StartingState());
    }

    void onNodeShutdown() {
        log.info("Shutdown hook called. Trying to terminate subscription gracefully");
        switchState(new CleanupState(null));
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
            } catch (final NakadiRuntimeException ex) {
                log.error("Failed to process task " + task + ", will rethrow original error", ex);
                switchState(new CleanupState(ex.getException()));
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

    public boolean isSubscriptionConsumptionBlocked() {
        return blacklistService.isSubscriptionConsumptionBlocked(
                subscription.getEventTypes(),
                parameters.getConsumingAppId());
    }

    public CursorTokenService getCursorTokenService() {
        return cursorTokenService;
    }

    public ObjectMapper getObjectMapper() {
        return objectMapper;
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

    public static final class Builder {
        private SubscriptionOutput out;
        private StreamParameters parameters;
        private Session session;
        private ScheduledExecutorService timer;
        private ZkSubscriptionClient zkClient;
        private BiFunction<Session[], Partition[], Partition[]> rebalancer;
        private long kafkaPollTimeout;
        private String loggingPath;
        private AtomicBoolean connectionReady;
        private CursorTokenService cursorTokenService;
        private ObjectMapper objectMapper;
        private BlacklistService blacklistService;
        private CursorConverter cursorConverter;
        private Subscription subscription;
        private MetricRegistry metricRegistry;
        private TimelineService timelineService;

        public Builder setOut(final SubscriptionOutput out) {
            this.out = out;
            return this;
        }

        public Builder setParameters(final StreamParameters parameters) {
            this.parameters = parameters;
            return this;
        }

        public Builder setSession(final Session session) {
            this.session = session;
            return this;
        }

        public Builder setTimer(final ScheduledExecutorService timer) {
            this.timer = timer;
            return this;
        }

        public Builder setZkClient(final ZkSubscriptionClient zkClient) {
            this.zkClient = zkClient;
            return this;
        }

        public Builder setRebalancer(final BiFunction<Session[], Partition[], Partition[]> rebalancer) {
            this.rebalancer = rebalancer;
            return this;
        }

        public Builder setKafkaPollTimeout(final long kafkaPollTimeout) {
            this.kafkaPollTimeout = kafkaPollTimeout;
            return this;
        }

        public Builder setLoggingPath(final String loggingPath) {
            this.loggingPath = loggingPath;
            return this;
        }

        public Builder setConnectionReady(final AtomicBoolean connectionReady) {
            this.connectionReady = connectionReady;
            return this;
        }

        public Builder setTimelineService(final TimelineService timelineService) {
            this.timelineService = timelineService;
            return this;
        }

        public Builder setCursorTokenService(final CursorTokenService cursorTokenService) {
            this.cursorTokenService = cursorTokenService;
            return this;
        }

        public Builder setObjectMapper(final ObjectMapper objectMapper) {
            this.objectMapper = objectMapper;
            return this;
        }

        public Builder setBlacklistService(final BlacklistService blacklistService) {
            this.blacklistService = blacklistService;
            return this;
        }

        public Builder setCursorConverter(final CursorConverter cursorConverter) {
            this.cursorConverter = cursorConverter;
            return this;
        }

        public Builder setSubscription(final Subscription subscription) {
            this.subscription = subscription;
            return this;
        }

        public Builder setMetricRegistry(final MetricRegistry metricRegistry) {
            this.metricRegistry = metricRegistry;
            return this;
        }

        public StreamingContext build() {
            return new StreamingContext(this);
        }
    }

}
