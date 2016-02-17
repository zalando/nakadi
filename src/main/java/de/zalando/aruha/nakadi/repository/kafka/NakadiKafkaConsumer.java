package de.zalando.aruha.nakadi.repository.kafka;

import com.google.common.collect.Lists;
import de.zalando.aruha.nakadi.domain.ConsumedEvent;
import de.zalando.aruha.nakadi.domain.Cursor;
import de.zalando.aruha.nakadi.repository.EventConsumer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static de.zalando.aruha.nakadi.repository.kafka.KafkaCursor.kafkaCursor;

public class NakadiKafkaConsumer implements EventConsumer {

    private final Consumer<String, String> kafkaConsumer;

    private final List<TopicPartition> topicPartitions;

    private Queue<ConsumedEvent> eventQueue;

    private final long pollTimeout;

    public NakadiKafkaConsumer(final KafkaFactory factory, final String topic, final Map<String, String> cursors,
                               final long pollTimeout) {
        eventQueue = Lists.newLinkedList();
        kafkaConsumer = factory.getConsumer();
        this.pollTimeout = pollTimeout;

        final List<KafkaCursor> kafkaCursors = cursors
                .entrySet()
                .stream()
                .map(entry -> kafkaCursor(entry.getKey(), entry.getValue()))
                .collect(Collectors.toList());

        // define topic/partitions to consume from
        topicPartitions = kafkaCursors
                .stream()
                .map(cursor -> new TopicPartition(topic, cursor.getPartition()))
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
    public Optional<ConsumedEvent> readEvent() {
        if (eventQueue.isEmpty()) {
            pollFromKafka();
        }
        return Optional.ofNullable(eventQueue.poll());
    }

    @Override
    public Map<String, String> fetchNextOffsets() {
        return topicPartitions
                .stream()
                .map(tp -> kafkaCursor(tp.partition(), kafkaConsumer.position(tp)).asNakadiCursor())
                .collect(Collectors.toMap(
                        Cursor::getPartition,
                        Cursor::getOffset));
    }

    private void pollFromKafka() {
        final ConsumerRecords<String, String> records = kafkaConsumer.poll(pollTimeout);
        eventQueue = StreamSupport
                .stream(records.spliterator(), false)
                .map(record -> {
                    final Cursor cursor = kafkaCursor(record.partition(), record.offset() + 1).asNakadiCursor();
                    return new ConsumedEvent(record.value(), record.topic(), cursor.getPartition(),
                            cursor.getOffset());
                })
                .collect(Collectors.toCollection(Lists::newLinkedList));
    }
}
