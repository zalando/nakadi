package de.zalando.aruha.nakadi.service.subscription;

import de.zalando.aruha.nakadi.domain.Subscription;
import de.zalando.aruha.nakadi.exceptions.NoSuchSubscriptionException;
import de.zalando.aruha.nakadi.repository.EventTypeRepository;
import de.zalando.aruha.nakadi.repository.db.SubscriptionDbRepository;
import de.zalando.aruha.nakadi.repository.kafka.KafkaTopicRepository;
import de.zalando.aruha.nakadi.repository.zookeeper.ZooKeeperHolder;
import de.zalando.aruha.nakadi.service.subscription.model.Session;
import de.zalando.aruha.nakadi.service.subscription.zk.CuratorZkSubscriptionClient;
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
            final AtomicBoolean connectionReady) throws NoSuchSubscriptionException {
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
