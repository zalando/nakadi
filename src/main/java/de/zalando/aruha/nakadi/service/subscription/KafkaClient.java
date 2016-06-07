package de.zalando.aruha.nakadi.service.subscription;

import de.zalando.aruha.nakadi.domain.Cursor;
import de.zalando.aruha.nakadi.domain.Subscription;
import de.zalando.aruha.nakadi.exceptions.NakadiException;
import de.zalando.aruha.nakadi.repository.kafka.KafkaTopicRepository;
import de.zalando.aruha.nakadi.service.subscription.model.Partition;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

public class KafkaClient {
    private final Subscription subscription;
    private final KafkaTopicRepository topicRepository;
    private Consumer<Exception> exceptionListener;

    KafkaClient(final Subscription subscription, final KafkaTopicRepository topicRepository) {
        this.subscription = subscription;
        this.topicRepository = topicRepository;
    }

    void setExceptionListener(final Consumer<Exception> exceptionListener) {
        this.exceptionListener = exceptionListener;
    }

    public Map<Partition.PartitionKey, Long> getSubscriptionOffsets() {
        final Map<Partition.PartitionKey, Long> offsets = new HashMap<>();
        try {
            for (final String eventType : subscription.getEventTypes()) {
                // TODO: Subscription do not have start position :( Will start streaming from the end.
                topicRepository.materializePositions(eventType, Cursor.AFTER_NEWEST_OFFSET).entrySet().forEach(
                        e -> offsets.put(new Partition.PartitionKey(eventType, e.getKey()), e.getValue()));
            }
            return offsets;
        } catch (final NakadiException e) {
            exceptionListener.accept(e);
            return null;
        }
    }

    public org.apache.kafka.clients.consumer.Consumer<String, String> createKafkaConsumer() {
        return topicRepository.createKafkaConsumer();
    }
}
