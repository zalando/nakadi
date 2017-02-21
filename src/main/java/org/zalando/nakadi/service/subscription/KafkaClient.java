package org.zalando.nakadi.service.subscription;

import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.exceptions.NakadiException;
import org.zalando.nakadi.exceptions.NakadiRuntimeException;
import org.zalando.nakadi.repository.EventTypeRepository;
import org.zalando.nakadi.repository.kafka.KafkaTopicRepository;
import org.zalando.nakadi.service.subscription.model.Partition;
import org.zalando.nakadi.service.timeline.TimelineService;

import java.util.HashMap;
import java.util.Map;

public class KafkaClient {

    private final Subscription subscription;
    private final EventTypeRepository eventTypeRepository;
    private final KafkaTopicRepository topicRepository;

    public KafkaClient(final Subscription subscription,
                       final TimelineService timelineService,
                       final EventTypeRepository eventTypeRepository) {
        this.subscription = subscription;
        this.eventTypeRepository = eventTypeRepository;
        // FIXME TIMELINE: for refactoring purposes, has to be removed during timeline event consumption task
        // for now we always will have the same topic repo, KafkaTopicRepository
        try {
            final EventType eventType = eventTypeRepository.findByName(subscription.getEventTypes().iterator().next());
            this.topicRepository = (KafkaTopicRepository) timelineService.getTopicRepository(eventType);
        } catch (final NakadiException e) {
            throw new NakadiRuntimeException(e);
        }
        // FIXME TIMELINE: for refactoring purposes, has to be removed during timeline event consumption task
    }

    public Map<Partition.PartitionKey, Long> getSubscriptionOffsets() {
        final Map<Partition.PartitionKey, Long> offsets = new HashMap<>();
        try {
            for (final String eventTypeName : subscription.getEventTypes()) {
                final EventType eventType = eventTypeRepository.findByName(eventTypeName);
                final String topic = eventType.getTopic();
                topicRepository.materializePositions(topic, subscription.getReadFrom())
                        .entrySet()
                        .forEach(
                                e -> offsets.put(new Partition.PartitionKey(topic, e.getKey()), e.getValue() - 1));
            }
            return offsets;
        } catch (final NakadiException e) {
            throw new NakadiRuntimeException(e);
        }
    }

    public org.apache.kafka.clients.consumer.Consumer<String, String> createKafkaConsumer() {
        return topicRepository.createKafkaConsumer();
    }
}
