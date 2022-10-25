package org.zalando.nakadi.service.subscription;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.zalando.nakadi.cache.EventTypeCache;
import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.exceptions.runtime.InternalNakadiException;
import org.zalando.nakadi.exceptions.runtime.NoSuchEventTypeException;
import org.zalando.nakadi.service.AuthorizationValidator;
import org.zalando.nakadi.service.ConsumptionKpiCollectorFactory;
import org.zalando.nakadi.service.CursorConverter;
import org.zalando.nakadi.service.CursorOperationsService;
import org.zalando.nakadi.service.CursorTokenService;
import org.zalando.nakadi.service.EventStreamChecks;
import org.zalando.nakadi.service.EventStreamWriterFactory;
import org.zalando.nakadi.service.EventTypeChangeListener;
import org.zalando.nakadi.service.NakadiCursorComparator;
import org.zalando.nakadi.service.subscription.model.Session;
import org.zalando.nakadi.service.subscription.zk.SubscriptionClientFactory;
import org.zalando.nakadi.service.subscription.zk.ZkSubscriptionClient;
import org.zalando.nakadi.service.timeline.TimelineService;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

@Service
public class SubscriptionStreamerFactory {
    @Value("${nakadi.kafka.poll.timeoutMs}")
    private long kafkaPollTimeout;
    private final TimelineService timelineService;
    private final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
    private final CursorTokenService cursorTokenService;
    private final ObjectMapper objectMapper;
    private final CursorConverter cursorConverter;
    private final MetricRegistry metricRegistry;
    private final SubscriptionClientFactory zkClientFactory;
    private final EventStreamWriterFactory eventStreamWriterFactory;
    private final AuthorizationValidator authorizationValidator;
    private final EventTypeChangeListener eventTypeChangeListener;
    private final EventTypeCache eventTypeCache;
    private final CursorOperationsService cursorOperationsService;
    private final EventStreamChecks eventStreamChecks;
    private final long streamMemoryLimitBytes;
    private final ConsumptionKpiCollectorFactory consumptionKpiCollectorFactory;

    @Autowired
    public SubscriptionStreamerFactory(
            final TimelineService timelineService,
            final CursorTokenService cursorTokenService,
            final ObjectMapper objectMapper,
            final CursorConverter cursorConverter,
            @Qualifier("streamMetricsRegistry") final MetricRegistry metricRegistry,
            final SubscriptionClientFactory zkClientFactory,
            final EventStreamWriterFactory eventStreamWriterFactory,
            final AuthorizationValidator authorizationValidator,
            final EventTypeChangeListener eventTypeChangeListener,
            final EventTypeCache eventTypeCache,
            final CursorOperationsService cursorOperationsService,
            final EventStreamChecks eventStreamChecks,
            @Value("${nakadi.subscription.maxStreamMemoryBytes}") final long streamMemoryLimitBytes,
            final ConsumptionKpiCollectorFactory consumptionKpiCollectorFactory) {
        this.timelineService = timelineService;
        this.cursorTokenService = cursorTokenService;
        this.objectMapper = objectMapper;
        this.cursorConverter = cursorConverter;
        this.metricRegistry = metricRegistry;
        this.zkClientFactory = zkClientFactory;
        this.eventStreamWriterFactory = eventStreamWriterFactory;
        this.authorizationValidator = authorizationValidator;
        this.eventTypeChangeListener = eventTypeChangeListener;
        this.eventTypeCache = eventTypeCache;
        this.cursorOperationsService = cursorOperationsService;
        this.eventStreamChecks = eventStreamChecks;
        this.streamMemoryLimitBytes = streamMemoryLimitBytes;
        this.consumptionKpiCollectorFactory = consumptionKpiCollectorFactory;
    }

    public SubscriptionStreamer build(
            final Subscription subscription,
            final StreamParameters streamParameters,
            final Session session,
            final SubscriptionOutput output,
            final StreamContentType streamContentType)
            throws InternalNakadiException, NoSuchEventTypeException {
        final ZkSubscriptionClient zkClient = zkClientFactory.createClient(
                subscription,
                streamParameters.commitTimeoutMillis);
        // Create streaming context
        return new StreamingContext.Builder()
                .setOut(output)
                .setStreamMemoryLimitBytes(streamMemoryLimitBytes)
                .setParameters(streamParameters)
                .setSession(session)
                .setTimer(executorService)
                .setZkClient(zkClient)
                .setRebalancer(new SubscriptionRebalancer())
                .setKafkaPollTimeout(kafkaPollTimeout)
                .setCursorTokenService(cursorTokenService)
                .setObjectMapper(objectMapper)
                .setEventStreamChecks(eventStreamChecks)
                .setCursorConverter(cursorConverter)
                .setSubscription(subscription)
                .setMetricRegistry(metricRegistry)
                .setTimelineService(timelineService)
                .setWriter(eventStreamWriterFactory.get(streamContentType))
                .setAuthorizationValidator(authorizationValidator)
                .setEventTypeChangeListener(eventTypeChangeListener)
                .setCursorComparator(new NakadiCursorComparator(eventTypeCache))
                .setKpiCollector(consumptionKpiCollectorFactory.createForHiLA(
                        subscription.getId(), streamParameters.getConsumingClient()))
                .setCursorOperationsService(cursorOperationsService)
                .build();
    }

}
