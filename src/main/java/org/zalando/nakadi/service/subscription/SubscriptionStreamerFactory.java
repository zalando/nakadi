package org.zalando.nakadi.service.subscription;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.exceptions.InternalNakadiException;
import org.zalando.nakadi.exceptions.NoSuchEventTypeException;
import org.zalando.nakadi.exceptions.NoSuchSubscriptionException;
import org.zalando.nakadi.exceptions.ServiceUnavailableException;
import org.zalando.nakadi.repository.EventTypeRepository;
import org.zalando.nakadi.repository.db.SubscriptionDbRepository;
import org.zalando.nakadi.repository.kafka.KafkaTopicRepository;
import org.zalando.nakadi.repository.zookeeper.ZooKeeperHolder;
import org.zalando.nakadi.service.CursorTokenService;
import org.zalando.nakadi.service.BlacklistService;
import org.zalando.nakadi.service.subscription.model.Session;
import org.zalando.nakadi.service.subscription.zk.CuratorZkSubscriptionClient;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

@Service
public class SubscriptionStreamerFactory {
    @Value("${nakadi.kafka.poll.timeoutMs}")
    private long kafkaPollTimeout;
    private final ZooKeeperHolder zkHolder;
    private final SubscriptionDbRepository subscriptionDbRepository;
    private final KafkaTopicRepository topicRepository;
    private final EventTypeRepository eventTypeRepository;
    private final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
    private final CursorTokenService cursorTokenService;
    private final ObjectMapper objectMapper;

    @Autowired
    public SubscriptionStreamerFactory(
            final ZooKeeperHolder zkHolder,
            final SubscriptionDbRepository subscriptionDbRepository,
            final KafkaTopicRepository topicRepository,
            final EventTypeRepository eventTypeRepository,
            final CursorTokenService cursorTokenService,
            final ObjectMapper objectMapper) {
        this.zkHolder = zkHolder;
        this.subscriptionDbRepository = subscriptionDbRepository;
        this.topicRepository = topicRepository;
        this.eventTypeRepository = eventTypeRepository;
        this.cursorTokenService = cursorTokenService;
        this.objectMapper = objectMapper;
    }

    public SubscriptionStreamer build(
            final String subscriptionId,
            final StreamParameters streamParameters,
            final SubscriptionOutput output,
            final AtomicBoolean connectionReady,
            final BlacklistService blacklistService) throws NoSuchSubscriptionException, ServiceUnavailableException,
            InternalNakadiException, NoSuchEventTypeException {

        final Subscription subscription = subscriptionDbRepository.getSubscription(subscriptionId);
        final Map<String, String> eventTypesForTopics = createTopicsMap(subscription.getEventTypes());
        final Session session = Session.generate(1);
        final String loggingPath = "subscription." + subscriptionId + "." + session.getId();
        // Create streaming context
        return new StreamingContext.Builder()
                .setOut(output)
                .setParameters(streamParameters)
                .setSession(session)
                .setTimer(executorService)
                .setZkClient(new CuratorZkSubscriptionClient(subscription.getId(), zkHolder.get(), loggingPath))
                .setKafkaClient(new KafkaClient(subscription, topicRepository, eventTypeRepository))
                .setRebalancer(new ExactWeightRebalancer())
                .setKafkaPollTimeout(kafkaPollTimeout)
                .setLoggingPath(loggingPath)
                .setConnectionReady(connectionReady)
                .setEventTypesForTopics(eventTypesForTopics)
                .setCursorTokenService(cursorTokenService)
                .setObjectMapper(objectMapper)
                .setBlacklistService(blacklistService)
                .build();
    }

    private Map<String, String> createTopicsMap(final Set<String> eventTypes) throws InternalNakadiException,
            NoSuchEventTypeException {
        final ImmutableMap.Builder<String, String> topicMapBuilder = ImmutableMap.builder();
        for (final String eventTypeName : eventTypes) {
            final EventType eventType = eventTypeRepository.findByName(eventTypeName);
            topicMapBuilder.put(eventType.getTopic(), eventTypeName);
        }
        return topicMapBuilder.build();
    }

}
