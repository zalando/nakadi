package org.zalando.nakadi.repository.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.zalando.nakadi.domain.KafkaSubscriptionSerializer;
import org.zalando.nakadi.domain.EventOwnerHeader;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.exceptions.runtime.InvalidCursorException;
import org.zalando.nakadi.repository.LowLevelConsumer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;

public class NakadiKafkaConsumer implements LowLevelConsumer {

    private final Consumer<byte[], byte[]> kafkaConsumer;
    private final long pollTimeout;

    public NakadiKafkaConsumer(
            final Consumer<byte[], byte[]> kafkaConsumer,
            final long pollTimeout) {
        this.kafkaConsumer = kafkaConsumer;
        this.pollTimeout = pollTimeout;
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
        kafkaConsumer.assign(Collections.emptyList());

        assign(cursors.stream()
                .map(NakadiCursor::asKafkaCursor)
                // because Nakadi `BEGIN` offset is -1
                .map(kafkaCursor -> kafkaCursor.addOffset(1))
                .collect(toList()));
    }

    @Override
    public List<LowLevelConsumer.Event> readEvents() {
        final ConsumerRecords<byte[], byte[]> records = kafkaConsumer.poll(pollTimeout);
        if (records.isEmpty()) {
            return Collections.emptyList();
        }
        final List<Event> result = new ArrayList<>(records.count());
        for (final ConsumerRecord<byte[], byte[]> record : records) {
            result.add(new Event(
                    record.value(),
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    record.timestamp(),
                    EventOwnerHeader.deserialize(record),
                    KafkaSubscriptionSerializer.deserialize(record)));
        }
        return result;
    }

    @Override
    public void close() {
        kafkaConsumer.close();
    }

}
