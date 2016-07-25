package org.zalando.nakadi.service.subscription;

import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.repository.EventTypeRepository;
import org.zalando.nakadi.repository.TopicRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class SubscriptionKafkaClientFactory {

    private final TopicRepository topicRepository;
    private final EventTypeRepository eventTypeRepository;

    @Autowired
    public SubscriptionKafkaClientFactory(final TopicRepository topicRepository,
                                          final EventTypeRepository eventTypeRepository) {
        this.topicRepository = topicRepository;
        this.eventTypeRepository = eventTypeRepository;
    }

    public KafkaClient createKafkaClient(final Subscription subscription) {
        return new KafkaClient(subscription, topicRepository, eventTypeRepository);
    }
}
