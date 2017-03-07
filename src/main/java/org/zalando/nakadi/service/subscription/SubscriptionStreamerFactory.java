package org.zalando.nakadi.service.subscription;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.exceptions.InternalNakadiException;
import org.zalando.nakadi.exceptions.NoSuchEventTypeException;
import org.zalando.nakadi.exceptions.NoSuchSubscriptionException;
import org.zalando.nakadi.exceptions.ServiceUnavailableException;
import org.zalando.nakadi.repository.EventTypeRepository;
import org.zalando.nakadi.repository.db.SubscriptionDbRepository;
import org.zalando.nakadi.repository.zookeeper.ZooKeeperHolder;
import org.zalando.nakadi.service.BlacklistService;
import org.zalando.nakadi.service.CursorConverter;
import org.zalando.nakadi.service.CursorTokenService;
import org.zalando.nakadi.service.subscription.model.Session;
import org.zalando.nakadi.service.subscription.zk.CuratorZkSubscriptionClient;
import org.zalando.nakadi.service.timeline.TimelineService;

@Service
public class SubscriptionStreamerFactory {
    @Value("${nakadi.kafka.poll.timeoutMs}")
    private long kafkaPollTimeout;
    private final ZooKeeperHolder zkHolder;
    private final SubscriptionDbRepository subscriptionDbRepository;
    private final TimelineService timelineService;
    private final EventTypeRepository eventTypeRepository;
    private final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
    private final CursorTokenService cursorTokenService;
    private final ObjectMapper objectMapper;
    private final CursorConverter cursorConverter;
    private final MetricRegistry metricRegistry;

    @Autowired
    public SubscriptionStreamerFactory(
            final ZooKeeperHolder zkHolder,
            final SubscriptionDbRepository subscriptionDbRepository,
            final TimelineService timelineService,
            final EventTypeRepository eventTypeRepository,
            final CursorTokenService cursorTokenService,
            final ObjectMapper objectMapper,
            final CursorConverter cursorConverter,
            @Qualifier("streamMetricsRegistry") final MetricRegistry metricRegistry) {
        this.zkHolder = zkHolder;
        this.subscriptionDbRepository = subscriptionDbRepository;
        this.timelineService = timelineService;
        this.eventTypeRepository = eventTypeRepository;
        this.cursorTokenService = cursorTokenService;
        this.objectMapper = objectMapper;
        this.cursorConverter = cursorConverter;
        this.metricRegistry = metricRegistry;
    }

    public SubscriptionStreamer build(
            final String subscriptionId,
            final StreamParameters streamParameters,
            final SubscriptionOutput output,
            final AtomicBoolean connectionReady,
            final BlacklistService blacklistService) throws NoSuchSubscriptionException, ServiceUnavailableException,
            InternalNakadiException, NoSuchEventTypeException {

        final Subscription subscription = subscriptionDbRepository.getSubscription(subscriptionId);
        final Map<String, Timeline> timelinesForTopics = createTimelinesMap(subscription.getEventTypes());
        final Session session = Session.generate(1);
        final String loggingPath = "subscription." + subscriptionId + "." + session.getId();
        // Create streaming context
        return new StreamingContext.Builder()
                .setOut(output)
                .setParameters(streamParameters)
                .setSession(session)
                .setTimer(executorService)
                .setZkClient(new CuratorZkSubscriptionClient(subscription.getId(), zkHolder.get(), loggingPath))
                .setKafkaClient(new KafkaClient(subscription, timelineService, cursorConverter))
                .setRebalancer(new ExactWeightRebalancer())
                .setKafkaPollTimeout(kafkaPollTimeout)
                .setLoggingPath(loggingPath)
                .setConnectionReady(connectionReady)
                .setEventTypesForTopics(timelinesForTopics)
                .setCursorTokenService(cursorTokenService)
                .setObjectMapper(objectMapper)
                .setBlacklistService(blacklistService)
                .setCursorConverter(cursorConverter)
                .setSubscriptionId(subscriptionId)
                .setMetricRegistry(metricRegistry)
                .build();
    }

    private Map<String, Timeline> createTimelinesMap(final Set<String> eventTypes) throws InternalNakadiException,
            NoSuchEventTypeException {
        final ImmutableMap.Builder<String, Timeline> topicMapBuilder = ImmutableMap.builder();
        for (final String eventTypeName : eventTypes) {
            final Timeline timeline = timelineService.getTimeline(eventTypeRepository.findByName(eventTypeName));
            topicMapBuilder.put(timeline.getTopic(), timeline);
        }
        return topicMapBuilder.build();
    }

}
