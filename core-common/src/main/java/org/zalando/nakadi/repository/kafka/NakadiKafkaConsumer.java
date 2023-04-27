package org.zalando.nakadi.repository.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.zalando.nakadi.domain.ConsumedEvent;
import org.zalando.nakadi.domain.EventOwnerHeader;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.PartitionStatistics;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.exceptions.runtime.InvalidCursorException;
import org.zalando.nakadi.exceptions.runtime.ServiceTemporarilyUnavailableException;
import org.zalando.nakadi.repository.EventConsumer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;
import static org.zalando.nakadi.domain.CursorError.NULL_OFFSET;
import static org.zalando.nakadi.domain.CursorError.NULL_PARTITION;
import static org.zalando.nakadi.domain.CursorError.PARTITION_NOT_FOUND;
import static org.zalando.nakadi.domain.CursorError.UNAVAILABLE;

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
    public void reassign(final Collection<NakadiCursor> cursors, List<PartitionStatistics> statistics)
            throws InvalidCursorException {
        final Map<NakadiCursor, KafkaCursor> cursorMapping = convertToKafkaCursors(cursors, statistics);
        final Map<TopicPartition, Timeline> timelineMap = cursorMapping.entrySet().stream()
                .collect(Collectors.toMap(
                        entry -> new TopicPartition(entry.getValue().getTopic(), entry.getValue().getPartition()),
                        entry -> entry.getKey().getTimeline(),
                        (v1, v2) -> v2));
        final List<KafkaCursor> kafkaCursors = cursorMapping.values().stream()
                .map(kafkaCursor -> kafkaCursor.addOffset(1))
                .collect(toList());

        timelineMap.clear();
        timelineMap.putAll(timelineMap);
        kafkaConsumer.assign(Collections.emptyList());
        assign(kafkaCursors);
    }

    private Map<NakadiCursor, KafkaCursor> convertToKafkaCursors(final Collection<NakadiCursor> cursors,
                                                                 final List<PartitionStatistics> statistics)
            throws ServiceTemporarilyUnavailableException, InvalidCursorException {
        final Map<NakadiCursor, KafkaCursor> result = new HashMap<>();
        for (final NakadiCursor position : cursors) {
            validateCursorForNulls(position);
            final Optional<PartitionStatistics> partition =
                    statistics.stream().filter(t -> Objects.equals(t.getPartition(), position.getPartition()))
                            .filter(t -> Objects.equals(t.getTimeline().getTopic(), position.getTopic()))
                            .findAny();
            if (!partition.isPresent()) {
                throw new InvalidCursorException(PARTITION_NOT_FOUND, position);
            }
            final KafkaCursor toCheck = position.asKafkaCursor();

            // Checking oldest position
            final KafkaCursor oldestCursor = KafkaCursor.fromNakadiCursor(partition.get().getBeforeFirst());
            if (toCheck.compareTo(oldestCursor) < 0) {
                throw new InvalidCursorException(UNAVAILABLE, position);
            }
            // checking newest position
            final KafkaCursor newestPosition = KafkaCursor.fromNakadiCursor(partition.get().getLast());
            if (toCheck.compareTo(newestPosition) > 0) {
                throw new InvalidCursorException(UNAVAILABLE, position);
            } else {
                result.put(position, toCheck);
            }
        }
        return result;
    }

    private void validateCursorForNulls(final NakadiCursor cursor) throws InvalidCursorException {
        if (cursor.getPartition() == null) {
            throw new InvalidCursorException(NULL_PARTITION, cursor);
        }
        if (cursor.getOffset() == null) {
            throw new InvalidCursorException(NULL_OFFSET, cursor);
        }
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
