package org.zalando.nakadi.repository.kafka;

import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
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
    private final Timeline timeline;

    public NakadiKafkaConsumer(
            final Consumer<String, String> kafkaConsumer,
            final List<KafkaCursor> kafkaCursors,
            final long pollTimeout,
            final Timeline timeline) {
        this.kafkaConsumer = kafkaConsumer;
        this.pollTimeout = pollTimeout;
        this.timeline = timeline;
        eventQueue = Lists.newLinkedList();
        // define topic/partitions to consume from
        final Map<TopicPartition, KafkaCursor> topicCursors = kafkaCursors.stream().collect(
                Collectors.toMap(
                        cursor -> new TopicPartition(cursor.getTopic(), cursor.getPartition()),
                        cursor -> cursor
                ));
        kafkaConsumer.assign(new ArrayList<>(topicCursors.keySet()));
        topicCursors.forEach((topicPartition, cursor) -> kafkaConsumer.seek(topicPartition, cursor.getOffset()));
    }

    @Override
    public Optional<ConsumedEvent> readEvent() {
        if (eventQueue.isEmpty()) {
            pollFromKafka();
        }
        return Optional.ofNullable(eventQueue.poll());
    }

    @Override
    public Consumer<String, String> getConsumer() {
        return kafkaConsumer;
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
                    return new ConsumedEvent(record.value(), cursor.toNakadiCursor(timeline));
                })
                .collect(Collectors.toCollection(Lists::newLinkedList));
    }
}
