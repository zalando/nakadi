package org.zalando.nakadi.service.subscription;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import io.opentracing.Span;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zalando.nakadi.ShutdownHooks;
import org.zalando.nakadi.domain.ConsumedEvent;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.exceptions.runtime.AccessDeniedException;
import org.zalando.nakadi.exceptions.runtime.NakadiRuntimeException;
import org.zalando.nakadi.exceptions.runtime.RebalanceConflictException;
import org.zalando.nakadi.service.AuthorizationValidator;
import org.zalando.nakadi.service.CursorConverter;
import org.zalando.nakadi.service.CursorOperationsService;
import org.zalando.nakadi.service.CursorTokenService;
import org.zalando.nakadi.service.EventStreamChecks;
import org.zalando.nakadi.service.EventStreamWriter;
import org.zalando.nakadi.service.EventTypeChangeListener;
import org.zalando.nakadi.service.publishing.NakadiKpiPublisher;
import org.zalando.nakadi.service.subscription.autocommit.AutocommitSupport;
import org.zalando.nakadi.service.subscription.model.Partition;
import org.zalando.nakadi.service.subscription.model.Session;
import org.zalando.nakadi.service.subscription.state.CleanupState;
import org.zalando.nakadi.service.subscription.state.DummyState;
import org.zalando.nakadi.service.subscription.state.StartingState;
import org.zalando.nakadi.service.subscription.state.State;
import org.zalando.nakadi.service.subscription.zk.ZkSubscription;
import org.zalando.nakadi.service.subscription.zk.ZkSubscriptionClient;
import org.zalando.nakadi.service.timeline.TimelineService;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;

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
    private final EventStreamChecks eventStreamChecks;
    private final ScheduledExecutorService timer;
    private final BlockingQueue<Runnable> taskQueue = new LinkedBlockingQueue<>();
    private final BiFunction<Collection<Session>, Partition[], Partition[]> rebalancer;
    private final CursorConverter cursorConverter;
    private final Subscription subscription;
    private final MetricRegistry metricRegistry;
    private final EventStreamWriter writer;
    private final AuthorizationValidator authorizationValidator;
    private final EventTypeChangeListener eventTypeChangeListener;
    private final Comparator<NakadiCursor> cursorComparator;
    private final NakadiKpiPublisher kpiPublisher;
    private final AutocommitSupport autocommitSupport;
    private final Span currentSpan;
    private final String kpiDataStreamedEventType;
    private final CursorOperationsService cursorOperationsService;

    private final long kpiCollectionFrequencyMs;

    private final long streamMemoryLimitBytes;

    private State currentState = new DummyState();
    private ZkSubscription<List<String>> sessionListSubscription;
    private Closeable authorizationCheckSubscription;
    private boolean sessionRegistered;
    private boolean zkClientClosed;

    private final Logger log;

    private StreamingContext(final Builder builder) {
        this.out = builder.out;
        this.parameters = builder.parameters;
        this.session = builder.session;
        this.rebalancer = builder.rebalancer;
        this.timer = builder.timer;
        this.zkClient = builder.zkClient;
        this.kafkaPollTimeout = builder.kafkaPollTimeout;
        this.log = LoggerFactory.getLogger(LogPathBuilder.build(builder.subscription.getId(), builder.session.getId()));
        this.connectionReady = builder.connectionReady;
        this.timelineService = builder.timelineService;
        this.cursorTokenService = builder.cursorTokenService;
        this.objectMapper = builder.objectMapper;
        this.eventStreamChecks = builder.eventStreamChecks;
        this.cursorConverter = builder.cursorConverter;
        this.subscription = builder.subscription;
        this.metricRegistry = builder.metricRegistry;
        this.writer = builder.writer;
        this.authorizationValidator = builder.authorizationValidator;
        this.eventTypeChangeListener = builder.eventTypeChangeListener;
        this.cursorComparator = builder.cursorComparator;
        this.kpiPublisher = builder.kpiPublisher;
        this.autocommitSupport = new AutocommitSupport(builder.cursorOperationsService, zkClient, cursorConverter);
        this.kpiDataStreamedEventType = builder.kpiDataStremedEventType;
        this.kpiCollectionFrequencyMs = builder.kpiCollectionFrequencyMs;
        this.streamMemoryLimitBytes = builder.streamMemoryLimitBytes;
        this.currentSpan = builder.currentSpan;
        this.cursorOperationsService = builder.cursorOperationsService;
    }

    public Span getCurrentSpan() {
        return currentSpan;
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

    public EventStreamWriter getWriter() {
        return this.writer;
    }

    public NakadiKpiPublisher getKpiPublisher() {
        return kpiPublisher;
    }

    public String getKpiDataStreamedEventType() {
        return kpiDataStreamedEventType;
    }

    public long getKpiCollectionFrequencyMs() {
        return kpiCollectionFrequencyMs;
    }

    public CursorOperationsService getCursorOperationsService() {
        return cursorOperationsService;
    }

    @Override
    public void stream() throws InterruptedException {
        try (Closeable ignore = ShutdownHooks.addHook(this::onNodeShutdown)) { // bugfix ARUHA-485
            streamInternal(new StartingState());
        } catch (final IOException ex) {
            log.error(
                    "Failed to delete shutdown hook for subscription {}. This method should not throw any exception",
                    getSubscription(),
                    ex);
        }
    }

    public AutocommitSupport getAutocommitSupport() {
        return autocommitSupport;
    }

    void onNodeShutdown() {
        log.info("Shutdown hook called. Trying to terminate subscription gracefully");
        switchState(new CleanupState(null));
    }

    void streamInternal(final State firstState)
            throws InterruptedException {
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
                switchStateImmediately(new CleanupState(ex.getException()));
            } catch (final RuntimeException ex) {
                log.error("Failed to process task " + task + ", code carefully!", ex);
                switchStateImmediately(new CleanupState(ex));
            }
        }
    }

    public void switchState(final State newState) {
        this.addTask(() -> {
            log.info("Switching state from {} to {}",
                    currentState.getClass().getSimpleName(),
                    newState.getClass().getSimpleName());
            // There is a problem with onExit call - it can not throw exceptions, otherwise it won't be possible
            // to finish state correctly. In order to avoid it in future state will be switched even in case of
            // exception.
            exitCurrentStateAndEnter(newState);
        });
    }

    public void switchStateImmediately(final State newState) {
        log.info("Cleaning task queue & Switching state immediately from {} to {}",
                currentState.getClass().getSimpleName(),
                newState.getClass().getSimpleName());
        taskQueue.clear();
        switchState(newState);
    }

    private void exitCurrentStateAndEnter(final State newState) {
        try {
            currentState.onExit();
        } finally {
            currentState = newState;
            currentState.setContext(this);
            currentState.onEnter();
        }
    }

    public void registerSession() throws NakadiRuntimeException {
        log.info("Registering session {}", session);
        zkClient.registerSession(session);
        sessionRegistered = true;
    }

    public void closeZkClient() throws IOException {
        if (!zkClientClosed) {
            zkClient.close();
            zkClientClosed = true;
        }
    }

    public void subscribeToSessionListChangeAndRebalance() throws NakadiRuntimeException {
        // Install re-balance hook on client list change.
        sessionListSubscription = zkClient.subscribeForSessionListChanges(() -> addTask(this::rebalance));
        // Trigger re-balance explicitly as session list might have changed before scheduling hook
        rebalance();
    }

    public void unregisterSession() {
        log.info("Unregistering session {}", session);
        try {
            if (sessionListSubscription != null) {
                sessionListSubscription.close();
            }
        } finally {
            this.sessionListSubscription = null;
            if (sessionRegistered) {
                sessionRegistered = false;
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
        return eventStreamChecks.isConsumptionBlocked(
                subscription.getEventTypes(),
                parameters.getConsumingClient().getClientId());
    }

    public boolean isConsumptionBlocked(final ConsumedEvent event) {
        return eventStreamChecks.isConsumptionBlocked(event);
    }

    public CursorTokenService getCursorTokenService() {
        return cursorTokenService;
    }

    public ObjectMapper getObjectMapper() {
        return objectMapper;
    }

    private void rebalance() {
        if (null != sessionListSubscription) {
            // This call is needed to renew subscription for session list changes.
            sessionListSubscription.getData();
            zkClient.updateTopology(topology -> {
                try {
                    return rebalancer.apply(
                            zkClient.listSessions(),
                            topology.getPartitions());
                } catch (final RebalanceConflictException e) {
                    log.warn("failed to rebalance partitions: {}", e.getMessage(), e);
                    return new Partition[0];
                }
            });
        }
    }

    public void unregisterAuthorizationUpdates() {
        if (null != authorizationCheckSubscription) {
            try {
                authorizationCheckSubscription.close();
            } catch (final IOException e) {
                log.error("Failed to cancel subscription for authorization updates. " +
                        "This operation should not throw exceptions at all", e);
            } finally {
                authorizationCheckSubscription = null;
            }
        }
    }

    public void registerForAuthorizationUpdates() {
        Preconditions.checkArgument(authorizationCheckSubscription == null);
        // In case of Authorization exception there will be a switch to CleanupState, cause it is a generic rule
        // for each task - switch to CleanupState with exception as a parameter
        // The reason for adding task is to execute this check on thread that still owns security context.
        authorizationCheckSubscription = eventTypeChangeListener.registerListener(
                (eventType) -> addTask(this::checkAccessAuthorized), subscription.getEventTypes());
    }

    public void checkAccessAuthorized() throws AccessDeniedException {
        this.authorizationValidator.authorizeSubscriptionView(subscription);
        this.authorizationValidator.authorizeSubscriptionRead(subscription);
    }

    public Comparator<NakadiCursor> getCursorComparator() {
        return cursorComparator;
    }

    public long getStreamMemoryLimitBytes() {
        return streamMemoryLimitBytes;
    }

    public static final class Builder {
        private SubscriptionOutput out;
        private StreamParameters parameters;
        private Session session;
        private ScheduledExecutorService timer;
        private ZkSubscriptionClient zkClient;
        private BiFunction<Collection<Session>, Partition[], Partition[]> rebalancer;
        private long kafkaPollTimeout;
        private AtomicBoolean connectionReady;
        private CursorTokenService cursorTokenService;
        private ObjectMapper objectMapper;
        private EventStreamChecks eventStreamChecks;
        private CursorConverter cursorConverter;
        private Subscription subscription;
        private MetricRegistry metricRegistry;
        private TimelineService timelineService;
        private EventStreamWriter writer;
        private AuthorizationValidator authorizationValidator;
        private EventTypeChangeListener eventTypeChangeListener;
        private Comparator<NakadiCursor> cursorComparator;
        private NakadiKpiPublisher kpiPublisher;
        private CursorOperationsService cursorOperationsService;
        private String kpiDataStremedEventType;
        private long kpiCollectionFrequencyMs;
        private long streamMemoryLimitBytes;
        private Span currentSpan;

        public Builder setCurrentSpan(final Span span) {
            this.currentSpan = span;
            return this;
        }

        public Builder setOut(final SubscriptionOutput out) {
            this.out = out;
            return this;
        }

        public Builder setStreamMemoryLimitBytes(final long streamMemoryLimitBytes) {
            this.streamMemoryLimitBytes = streamMemoryLimitBytes;
            return this;
        }

        public Builder setCursorComparator(final Comparator<NakadiCursor> comparator) {
            this.cursorComparator = comparator;
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

        public Builder setRebalancer(final BiFunction<Collection<Session>, Partition[], Partition[]> rebalancer) {
            this.rebalancer = rebalancer;
            return this;
        }

        public Builder setKafkaPollTimeout(final long kafkaPollTimeout) {
            this.kafkaPollTimeout = kafkaPollTimeout;
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

        public Builder setEventStreamChecks(final EventStreamChecks eventStreamChecks) {
            this.eventStreamChecks = eventStreamChecks;
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

        public Builder setWriter(final EventStreamWriter writer) {
            this.writer = writer;
            return this;
        }

        public Builder setAuthorizationValidator(final AuthorizationValidator authorizationValidator) {
            this.authorizationValidator = authorizationValidator;
            return this;
        }

        public Builder setEventTypeChangeListener(final EventTypeChangeListener eventTypeChangeListener) {
            this.eventTypeChangeListener = eventTypeChangeListener;
            return this;
        }

        public Builder setKpiPublisher(final NakadiKpiPublisher kpiPublisher) {
            this.kpiPublisher = kpiPublisher;
            return this;
        }

        public Builder setCursorOperationsService(final CursorOperationsService cursorOperationsService) {
            this.cursorOperationsService = cursorOperationsService;
            return this;
        }

        public Builder setKpiDataStremedEventType(final String kpiDataStremedEventType) {
            this.kpiDataStremedEventType = kpiDataStremedEventType;
            return this;
        }

        public Builder setKpiCollectionFrequencyMs(final long kpiCollectionFrequencyMs) {
            this.kpiCollectionFrequencyMs = kpiCollectionFrequencyMs;
            return this;
        }

        public StreamingContext build() {
            return new StreamingContext(this);
        }


    }

}
