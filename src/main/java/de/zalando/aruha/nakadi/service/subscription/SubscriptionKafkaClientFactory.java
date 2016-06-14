package de.zalando.aruha.nakadi.service.subscription;

import de.zalando.aruha.nakadi.domain.Subscription;
import de.zalando.aruha.nakadi.repository.TopicRepository;

public class SubscriptionKafkaClientFactory {

    private final TopicRepository topicRepository;

    public SubscriptionKafkaClientFactory(final TopicRepository topicRepository) {
        this.topicRepository = topicRepository;
    }

    public KafkaClient createKafkaClient(final Subscription subscription) {
        return new KafkaClient(subscription, topicRepository);
    }
}
