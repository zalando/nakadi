package org.zalando.nakadi.repository.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.zalando.nakadi.domain.ConsumedEvent;
import org.zalando.nakadi.domain.EventOwnerHeader;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.repository.EventConsumer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class NakadiKafkaConsumer implements EventConsumer.LowLevelConsumer {

    private final Consumer<byte[], byte[]> kafkaConsumer;
    private final long pollTimeout;
    private final Map<TopicPartition, Timeline> timelineMap;

    public NakadiKafkaConsumer(
            final Consumer<byte[], byte[]> kafkaConsumer,
            final List<KafkaCursor> kafkaCursors,
            final Map<TopicPartition, Timeline> timelineMap,
            final long pollTimeout) {
        this.kafkaConsumer = kafkaConsumer;
        this.pollTimeout = pollTimeout;
        this.timelineMap = timelineMap;
        // define topic/partitions to consume from
        final Map<TopicPartition, KafkaCursor> topicCursors = kafkaCursors.stream().collect(
                Collectors.toMap(
                        cursor -> new TopicPartition(cursor.getTopic(), cursor.getPartition()),
                        cursor -> cursor,
                        (cursor1, cursor2) -> cursor2
                ));
        kafkaConsumer.assign(new ArrayList<>(topicCursors.keySet()));
        topicCursors.forEach((topicPartition, cursor) -> kafkaConsumer.seek(topicPartition, cursor.getOffset()));
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
    public List<ConsumedEvent> readEvents() {
        final ConsumerRecords<byte[], byte[]> records = kafkaConsumer.poll(pollTimeout);
        if (records.isEmpty()) {
            return Collections.emptyList();
        }
        final ArrayList<ConsumedEvent> result = new ArrayList<>(records.count());
        for (final ConsumerRecord<byte[], byte[]> record : records) {
            final KafkaCursor cursor = new KafkaCursor(record.topic(), record.partition(), record.offset());
            final Timeline timeline = timelineMap.get(new TopicPartition(record.topic(), record.partition()));

            result.add(new ConsumedEvent(
                    record.value(),
                    cursor.toNakadiCursor(timeline),
                    record.timestamp(),
                    EventOwnerHeader.deserialize(record)));
        }
        return result;
    }

    @Override
    public void close() {
        kafkaConsumer.close();
    }

}
