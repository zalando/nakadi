package de.zalando.aruha.nakadi.repository.kafka;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.InvalidOffsetException;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;

import com.google.common.collect.Lists;

import de.zalando.aruha.nakadi.NakadiException;
import de.zalando.aruha.nakadi.domain.ConsumedEvent;
import de.zalando.aruha.nakadi.repository.EventConsumer;

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

        // define topic/partitions to consume from
        topicPartitions = cursors
                .keySet()
                .stream()
                .map(partition -> new TopicPartition(topic, Integer.parseInt(partition)))
                .collect(Collectors.toList());
        kafkaConsumer.assign(topicPartitions);

        // set offsets
        topicPartitions.forEach(topicPartition ->
                kafkaConsumer.seek(topicPartition,
                        Long.parseLong(cursors.get(Integer.toString(topicPartition.partition())))));
    }

    @Override
    public Optional<ConsumedEvent> readEvent() throws NakadiException {
        if (eventQueue.isEmpty()) {
            pollFromKafka();
        }
        return Optional.ofNullable(eventQueue.poll());
    }

    @Override
    public Map<String, String> fetchNextOffsets() {
        return topicPartitions
                .stream()
                .collect(Collectors.toMap(
                        topicPartition -> Integer.toString(topicPartition.partition()),
                        topicPartition -> Long.toString(kafkaConsumer.position(topicPartition))));
    }

    private void pollFromKafka() throws NakadiException {
        try {
            final ConsumerRecords<String, String> records = kafkaConsumer.poll(pollTimeout);
            eventQueue = StreamSupport
                    .stream(records.spliterator(), false)
                    .map(record -> {
                        System.out.println("Read record with offset:" + record.offset());
                        return new ConsumedEvent(record.value(), record.topic(),
                                Integer.toString(record.partition()), Long.toString(record.offset() + 1));

                    })
                    .collect(Collectors.toCollection(Lists::newLinkedList));
        } catch (InvalidOffsetException e) {
            throw new NakadiException("Wrong offset provided", e);
        } catch (KafkaException e) {
            throw new NakadiException("Error occurred when polling from kafka", e);
        }
    }
}
