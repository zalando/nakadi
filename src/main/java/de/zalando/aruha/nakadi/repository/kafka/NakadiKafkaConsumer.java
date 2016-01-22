package de.zalando.aruha.nakadi.repository.kafka;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import de.zalando.aruha.nakadi.domain.Cursor;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.InvalidOffsetException;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;

import com.google.common.collect.Lists;

import de.zalando.aruha.nakadi.NakadiException;
import de.zalando.aruha.nakadi.domain.ConsumedEvent;
import de.zalando.aruha.nakadi.repository.EventConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static de.zalando.aruha.nakadi.domain.TopicPartition.topicPartition;

/**
 * Additional layer over KafkaConsumer
 * This class is not thread safe as the KafkaConsumer it uses is also not thread safe
 */
public class NakadiKafkaConsumer implements EventConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(NakadiKafkaConsumer.class);

    private final Consumer<String, String> kafkaConsumer;

    private List<TopicPartition> topicPartitions;

    private Queue<ConsumedEvent> eventQueue;

    private final long pollTimeout;

    public NakadiKafkaConsumer(final KafkaFactory factory, final List<Cursor> cursors, final long pollTimeout) {
        kafkaConsumer = factory.getConsumer();
        this.pollTimeout = pollTimeout;
        setCursors(cursors);
    }

    @Override
    public void setCursors(final List<Cursor> cursors) {
        // define topic/partitions to consume from
        topicPartitions = cursors
                .stream()
                .map(cursor -> new TopicPartition(cursor.getTopic(), Integer.parseInt(cursor.getPartition())))
                .collect(Collectors.toList());
        kafkaConsumer.assign(topicPartitions);

        // set offsets
        cursors.forEach(cursor ->
                kafkaConsumer.seek(new TopicPartition(cursor.getTopic(), Integer.parseInt(cursor.getPartition())),
                        Long.parseLong(cursor.getOffset())));

        eventQueue = Lists.newLinkedList();

        LOG.info("Consumer is now configured with cursors: " + cursors);
    }

    @Override
    public Optional<ConsumedEvent> readEvent() throws NakadiException {
        if (eventQueue.isEmpty()) {
            pollFromKafka();
        }

        return Optional.ofNullable(eventQueue.poll());
    }

    @Override
    public Map<de.zalando.aruha.nakadi.domain.TopicPartition, String> fetchNextOffsets() {
        return topicPartitions
                .stream()
                .collect(Collectors.toMap(
                        tp -> topicPartition(tp.topic(), Integer.toString(tp.partition())),
                        tp -> Long.toString(kafkaConsumer.position(tp))));
    }

    private void pollFromKafka() throws NakadiException {
        try {
            final ConsumerRecords<String, String> records = kafkaConsumer.poll(pollTimeout);
            eventQueue = StreamSupport
                    .stream(records.spliterator(), false)
                    .map(record -> new ConsumedEvent(record.value(),
                                    topicPartition(record.topic(), Integer.toString(record.partition())),
                                    Long.toString(record.offset() + 1)))
                    .collect(Collectors.toCollection(Lists::newLinkedList));
        } catch (InvalidOffsetException e) {
            throw new NakadiException("Wrong offset provided", e);
        } catch (KafkaException e) {
            throw new NakadiException("Error occurred when polling from kafka", e);
        }
    }
}
