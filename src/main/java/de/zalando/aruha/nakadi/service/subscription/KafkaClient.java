package de.zalando.aruha.nakadi.service.subscription;

import de.zalando.aruha.nakadi.domain.Subscription;
import de.zalando.aruha.nakadi.exceptions.ExceptionWrapper;
import de.zalando.aruha.nakadi.exceptions.NakadiException;
import de.zalando.aruha.nakadi.repository.TopicRepository;
import de.zalando.aruha.nakadi.repository.kafka.KafkaTopicRepository;
import de.zalando.aruha.nakadi.service.subscription.model.Partition;
import java.util.HashMap;
import java.util.Map;

public class KafkaClient {
    private final Subscription subscription;
    private final KafkaTopicRepository topicRepository;

    public KafkaClient(final Subscription subscription, final TopicRepository topicRepository) {
        this.subscription = subscription;
        this.topicRepository = (KafkaTopicRepository) topicRepository;
    }

    public Map<Partition.PartitionKey, Long> getSubscriptionOffsets() {
        final Map<Partition.PartitionKey, Long> offsets = new HashMap<>();
        try {
            for (final String eventType : subscription.getEventTypes()) {
                topicRepository.materializePositions(eventType, subscription.getStartFrom()).entrySet().forEach(
                        e -> offsets.put(new Partition.PartitionKey(eventType, e.getKey()), e.getValue()));
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
