package de.zalando.aruha.nakadi.service.subscription;

import de.zalando.aruha.nakadi.domain.Subscription;
import de.zalando.aruha.nakadi.exceptions.NoSuchSubscriptionException;
import de.zalando.aruha.nakadi.repository.db.SubscriptionDbRepository;
import de.zalando.aruha.nakadi.repository.kafka.KafkaTopicRepository;
import de.zalando.aruha.nakadi.repository.zookeeper.ZooKeeperHolder;
import de.zalando.aruha.nakadi.service.subscription.model.Session;
import de.zalando.aruha.nakadi.service.subscription.zk.CuratorZkSubscriptionClient;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
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
    private final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();

    @Autowired
    public SubscriptionStreamerFactory(
            final ZooKeeperHolder zkHolder,
            final SubscriptionDbRepository subscriptionDbRepository,
            final KafkaTopicRepository topicRepository) {
        this.zkHolder = zkHolder;
        this.subscriptionDbRepository = subscriptionDbRepository;
        this.topicRepository = topicRepository;
    }

    public SubscriptionStreamer build(
            final String subscriptionId,
            final StreamParameters streamParameters,
            final SubscriptionOutput output) throws NoSuchSubscriptionException {

        // Make it here to trigger subscription not found exception earlier.
        final Subscription subscription = subscriptionDbRepository.getSubscription(subscriptionId);

        // Generate session
        final Session session = Session.generate(1);

        // Create curator zk client
        final CuratorZkSubscriptionClient zkClient = new CuratorZkSubscriptionClient(subscription.getId(), zkHolder.get());

        final KafkaClient kafkaClient = new KafkaClient(subscription, topicRepository);

        // Create streaming context
        final StreamingContext streamer = new StreamingContext(
                output,
                streamParameters,
                session,
                executorService,
                zkClient,
                kafkaClient,
                new ExactWeightRebalancer(),
                kafkaPollTimeout);

        // register exception listener to die fast on zookeeper exceptions.
        zkClient.setExceptionListener(streamer::onZkException);
        kafkaClient.setExceptionListener(streamer::onKafkaException);

        return streamer;
    }

}
