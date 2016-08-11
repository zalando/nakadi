package org.zalando.nakadi.service.subscription;

import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.exceptions.NoSuchSubscriptionException;
import org.zalando.nakadi.exceptions.ServiceUnavailableException;
import org.zalando.nakadi.repository.EventTypeRepository;
import org.zalando.nakadi.repository.db.SubscriptionDbRepository;
import org.zalando.nakadi.repository.kafka.KafkaTopicRepository;
import org.zalando.nakadi.repository.zookeeper.ZooKeeperHolder;
import org.zalando.nakadi.service.subscription.model.Session;
import org.zalando.nakadi.service.subscription.zk.CuratorZkSubscriptionClient;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class SubscriptionStreamerFactory {
    @Value("${nakadi.kafka.poll.timeoutMs}")
    private long kafkaPollTimeout;
    private final ZooKeeperHolder zkHolder;
    private final SubscriptionDbRepository subscriptionDbRepository;
    private final KafkaTopicRepository topicRepository;
    private final EventTypeRepository eventTypeRepository;
    private final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();

    @Autowired
    public SubscriptionStreamerFactory(
            final ZooKeeperHolder zkHolder,
            final SubscriptionDbRepository subscriptionDbRepository,
            final KafkaTopicRepository topicRepository,
            final EventTypeRepository eventTypeRepository) {
        this.zkHolder = zkHolder;
        this.subscriptionDbRepository = subscriptionDbRepository;
        this.topicRepository = topicRepository;
        this.eventTypeRepository = eventTypeRepository;
    }

    public SubscriptionStreamer build(
            final String subscriptionId,
            final StreamParameters streamParameters,
            final SubscriptionOutput output,
            final AtomicBoolean connectionReady) throws NoSuchSubscriptionException, ServiceUnavailableException {
        final Subscription subscription = subscriptionDbRepository.getSubscription(subscriptionId);

        final Session session = Session.generate(1);
        final String loggingPath = "subscription." + subscriptionId + "." + session.getId();
        // Create streaming context
        return new StreamingContext(
                output,
                streamParameters,
                session,
                executorService,
                new CuratorZkSubscriptionClient(subscription.getId(), zkHolder.get(), loggingPath),
                new KafkaClient(subscription, topicRepository, eventTypeRepository),
                new ExactWeightRebalancer(),
                kafkaPollTimeout,
                loggingPath,
                connectionReady);
    }

}
