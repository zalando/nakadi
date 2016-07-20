package de.zalando.aruha.nakadi.service.subscription;

import de.zalando.aruha.nakadi.domain.Subscription;
import de.zalando.aruha.nakadi.repository.EventTypeRepository;
import de.zalando.aruha.nakadi.repository.TopicRepository;
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
