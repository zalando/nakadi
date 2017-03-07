package org.zalando.nakadi.repository.kafka;

import com.google.common.collect.Lists;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.zalando.nakadi.domain.ConsumedEvent;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.repository.EventConsumer;

public class NakadiKafkaConsumer implements EventConsumer {

    private Queue<ConsumedEvent> eventQueue;

    private final Consumer<String, String> kafkaConsumer;
    private final long pollTimeout;
    private final Map<TopicPartition, Timeline> timelineMap;

    public NakadiKafkaConsumer(
            final Consumer<String, String> kafkaConsumer,
            final List<KafkaCursor> kafkaCursors,
            final Map<TopicPartition, Timeline> timelineMap,
            final long pollTimeout) {
        this.kafkaConsumer = kafkaConsumer;
        this.pollTimeout = pollTimeout;
        this.timelineMap = timelineMap;
        eventQueue = Lists.newLinkedList();
        // define topic/partitions to consume from
        final List<TopicPartition> topicPartitions = kafkaCursors
                .stream()
                .map(cursor -> new TopicPartition(cursor.getTopic(), cursor.getPartition()))
                .collect(Collectors.toList());
        kafkaConsumer.assign(topicPartitions);

        // set offsets
        topicPartitions.forEach(topicPartition ->
                kafkaConsumer.seek(
                        topicPartition,
                        kafkaCursors
                                .stream()
                                .filter(cursor -> cursor.getPartition() == topicPartition.partition())
                                .findFirst()
                                .get()
                                .getOffset()
                ));
    }

    @Override
    public Set<org.zalando.nakadi.domain.TopicPartition> getAssignment() {
        return kafkaConsumer.assignment().stream()
                .map(tp -> new org.zalando.nakadi.domain.TopicPartition(
                        tp.topic(),
                        KafkaCursor.toNakadiPartition(tp.partition())))
                .collect(Collectors.toSet());
    }

    @Override
    public Optional<ConsumedEvent> readEvent() {
        if (eventQueue.isEmpty()) {
            pollFromKafka();
        }
        return Optional.ofNullable(eventQueue.poll());
    }

    @Override
    public void close() {
        kafkaConsumer.close();
    }

    private void pollFromKafka() {
        final ConsumerRecords<String, String> records = kafkaConsumer.poll(pollTimeout);
        eventQueue = StreamSupport
                .stream(records.spliterator(), false)
                .map(record -> {
                    final KafkaCursor cursor = new KafkaCursor(record.topic(), record.partition(), record.offset());
                    final Timeline timeline = timelineMap.get(new TopicPartition(record.topic(), record.partition()));
                    return new ConsumedEvent(record.value(), cursor.toNakadiCursor(timeline));
                })
                .collect(Collectors.toCollection(Lists::newLinkedList));
    }
}
