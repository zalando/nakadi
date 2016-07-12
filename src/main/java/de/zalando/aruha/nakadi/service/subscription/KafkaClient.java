package de.zalando.aruha.nakadi.service.subscription;

import de.zalando.aruha.nakadi.domain.EventType;
import de.zalando.aruha.nakadi.domain.Subscription;
import de.zalando.aruha.nakadi.exceptions.ExceptionWrapper;
import de.zalando.aruha.nakadi.exceptions.NakadiException;
import de.zalando.aruha.nakadi.repository.EventTypeRepository;
import de.zalando.aruha.nakadi.repository.TopicRepository;
import de.zalando.aruha.nakadi.repository.kafka.KafkaTopicRepository;
import de.zalando.aruha.nakadi.service.subscription.model.Partition;

import java.util.HashMap;
import java.util.Map;

public class KafkaClient {
    private final Subscription subscription;
    private final KafkaTopicRepository topicRepository;
    private final EventTypeRepository eventTypeRepository;

    public KafkaClient(final Subscription subscription, final TopicRepository topicRepository,
                       final EventTypeRepository eventTypeRepository) {
        this.subscription = subscription;
        this.topicRepository = (KafkaTopicRepository) topicRepository;
        this.eventTypeRepository = eventTypeRepository;
    }

    public Map<Partition.PartitionKey, Long> getSubscriptionOffsets() {
        final Map<Partition.PartitionKey, Long> offsets = new HashMap<>();
        try {
            for (final String eventTypeName : subscription.getEventTypes()) {
                final EventType eventType = eventTypeRepository.findByName(eventTypeName);
                final String topic = eventType.getTopic();
                topicRepository.materializePositions(topic, subscription.getStartFrom())
                        .entrySet()
                        .forEach(
                                e -> offsets.put(new Partition.PartitionKey(topic, e.getKey()), e.getValue() - 1));
            }
            return offsets;
        } catch (final NakadiException e) {
            throw new ExceptionWrapper(e);
        }
    }

    public org.apache.kafka.clients.consumer.Consumer<String, String> createKafkaConsumer() {
        return topicRepository.createKafkaConsumer();
    }
}
