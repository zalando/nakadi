package org.zalando.nakadi.repository.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.zalando.nakadi.domain.ConsumedEvent;
import org.zalando.nakadi.domain.EventOwnerHeader;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.exceptions.runtime.InvalidCursorException;
import org.zalando.nakadi.repository.EventConsumer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;

public class NakadiKafkaConsumer implements EventConsumer.LowLevelConsumer {

    private final Consumer<byte[], byte[]> kafkaConsumer;
    private final long pollTimeout;
    private final Map<TopicPartition, Timeline> timelineMap;

    public NakadiKafkaConsumer(
            final Consumer<byte[], byte[]> kafkaConsumer,
            final long pollTimeout) {
        this.kafkaConsumer = kafkaConsumer;
        this.pollTimeout = pollTimeout;
        this.timelineMap = new HashMap<>();
        // define topic/partitions to consume from
    }

    private void assign(final Collection<KafkaCursor> kafkaCursors) {
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
    public void reassign(final Collection<NakadiCursor> cursors)
            throws InvalidCursorException {

        final Map<NakadiCursor, KafkaCursor> cursorMapping = cursors.stream()
                .collect(Collectors.toMap(nc -> nc, NakadiCursor::asKafkaCursor));

        final Map<TopicPartition, Timeline> tpToTimelines = cursorMapping.entrySet().stream()
                .collect(Collectors.toMap(
                        entry -> new TopicPartition(entry.getValue().getTopic(), entry.getValue().getPartition()),
                        entry -> entry.getKey().getTimeline(),
                        (v1, v2) -> v2));

        this.timelineMap.clear();
        this.timelineMap.putAll(tpToTimelines);
        kafkaConsumer.assign(Collections.emptyList());

        final List<KafkaCursor> kafkaCursors = cursorMapping.values().stream()
                // because Nakadi `BEGIN` offset is -1
                .map(kafkaCursor -> kafkaCursor.addOffset(1))
                .collect(toList());

        assign(kafkaCursors);
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
