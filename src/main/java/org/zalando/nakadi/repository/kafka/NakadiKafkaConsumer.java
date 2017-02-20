package org.zalando.nakadi.repository.kafka;

import com.google.common.collect.Lists;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.zalando.nakadi.domain.ConsumedEvent;
import org.zalando.nakadi.repository.EventConsumer;

public class NakadiKafkaConsumer implements EventConsumer {

    private final Consumer<String, String> kafkaConsumer;

    private Queue<ConsumedEvent> eventQueue;

    private final long pollTimeout;

    public NakadiKafkaConsumer(final Consumer<String, String> kafkaConsumer, final List<KafkaCursor> kafkaCursors,
                               final long pollTimeout) {
        eventQueue = Lists.newLinkedList();
        this.kafkaConsumer = kafkaConsumer;
        this.pollTimeout = pollTimeout;

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
    public Optional<ConsumedEvent> readEvent() {
        if (eventQueue.isEmpty()) {
            pollFromKafka();
        }
        kafkaConsumer.commitAsync();
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
                    return new ConsumedEvent(record.value(), cursor.toNakadiCursor());
                })
                .collect(Collectors.toCollection(Lists::newLinkedList));
    }
}
