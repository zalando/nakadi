package org.zalando.nakadi.service.subscription;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.exceptions.InternalNakadiException;
import org.zalando.nakadi.exceptions.NoSuchEventTypeException;
import org.zalando.nakadi.exceptions.NoSuchSubscriptionException;
import org.zalando.nakadi.exceptions.ServiceUnavailableException;
import org.zalando.nakadi.service.AuthorizationValidator;
import org.zalando.nakadi.service.BlacklistService;
import org.zalando.nakadi.service.CursorConverter;
import org.zalando.nakadi.service.CursorTokenService;
import org.zalando.nakadi.service.EventStreamWriterProvider;
import org.zalando.nakadi.service.EventTypeChangeListener;
import org.zalando.nakadi.service.subscription.model.Session;
import org.zalando.nakadi.service.subscription.zk.SubscriptionClientFactory;
import org.zalando.nakadi.service.timeline.TimelineService;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

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
    private final EventStreamWriterProvider eventStreamWriterProvider;
    private final AuthorizationValidator authorizationValidator;
    private final EventTypeChangeListener eventTypeChangeListener;

    @Autowired
    public SubscriptionStreamerFactory(
            final TimelineService timelineService,
            final CursorTokenService cursorTokenService,
            final ObjectMapper objectMapper,
            final CursorConverter cursorConverter,
            @Qualifier("streamMetricsRegistry") final MetricRegistry metricRegistry,
            final SubscriptionClientFactory zkClientFactory,
            final EventStreamWriterProvider eventStreamWriterProvider,
            final AuthorizationValidator authorizationValidator,
            final EventTypeChangeListener eventTypeChangeListener) {
        this.timelineService = timelineService;
        this.cursorTokenService = cursorTokenService;
        this.objectMapper = objectMapper;
        this.cursorConverter = cursorConverter;
        this.metricRegistry = metricRegistry;
        this.zkClientFactory = zkClientFactory;
        this.eventStreamWriterProvider = eventStreamWriterProvider;
        this.authorizationValidator = authorizationValidator;
        this.eventTypeChangeListener = eventTypeChangeListener;
    }

    public SubscriptionStreamer build(
            final Subscription subscription,
            final StreamParameters streamParameters,
            final SubscriptionOutput output,
            final AtomicBoolean connectionReady,
            final BlacklistService blacklistService)
            throws NoSuchSubscriptionException, ServiceUnavailableException,
            InternalNakadiException, NoSuchEventTypeException {
        final Session session = Session.generate(1);
        final String loggingPath = "subscription." + subscription.getId() + "." + session.getId();
        // Create streaming context
        return new StreamingContext.Builder()
                .setOut(output)
                .setParameters(streamParameters)
                .setSession(session)
                .setTimer(executorService)
                .setZkClient(zkClientFactory.createClient(subscription, loggingPath))
                .setRebalancer(new ExactWeightRebalancer())
                .setKafkaPollTimeout(kafkaPollTimeout)
                .setLoggingPath(loggingPath)
                .setConnectionReady(connectionReady)
                .setCursorTokenService(cursorTokenService)
                .setObjectMapper(objectMapper)
                .setBlacklistService(blacklistService)
                .setCursorConverter(cursorConverter)
                .setSubscription(subscription)
                .setMetricRegistry(metricRegistry)
                .setTimelineService(timelineService)
                .setWriter(eventStreamWriterProvider.getWriter())
                .setAuthorizationValidator(authorizationValidator)
                .setEventTypeChangeListener(eventTypeChangeListener)
                .build();
    }

}
