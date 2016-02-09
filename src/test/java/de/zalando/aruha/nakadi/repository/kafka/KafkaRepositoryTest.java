package de.zalando.aruha.nakadi.repository.kafka;

import de.zalando.aruha.nakadi.domain.Cursor;
import de.zalando.aruha.nakadi.domain.TopicPartition;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.PartitionInfo;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class KafkaRepositoryTest {

    public static final String MY_TOPIC = "my-topic";

    private static final Set<PartitionState> PARTITIONS;

    public static final long LATEST_TIME = kafka.api.OffsetRequest.LatestTime();

    public static final long EARLIEST_TIME = kafka.api.OffsetRequest.EarliestTime();

    public static final String ANOTHER_TOPIC = "another-topic";

    private final KafkaRepository kafkaRepository = createKafkaRepository();

    static {
        PARTITIONS = new HashSet<>();

        PARTITIONS.add(new PartitionState(MY_TOPIC, 0, 40, 42));
        PARTITIONS.add(new PartitionState(MY_TOPIC, 1, 100, 200));

        PARTITIONS.add(new PartitionState(ANOTHER_TOPIC, 1, 0, 100));
        PARTITIONS.add(new PartitionState(ANOTHER_TOPIC, 5, 12, 60));
        PARTITIONS.add(new PartitionState(ANOTHER_TOPIC, 9, 99, 222));
    }


    @Test
    public void canListAllPartitions() {
        canListAllPartitionsOfTopic(MY_TOPIC);
        canListAllPartitionsOfTopic(ANOTHER_TOPIC);
    }

    private void canListAllPartitionsOfTopic(final String topic) {
        final List<TopicPartition> expected = PARTITIONS.stream().filter(p -> p.topic.equals(topic))
                .map(p -> {
                    final TopicPartition topicPartition = new TopicPartition(p.topic,
                            String.valueOf(p.partition));
                    topicPartition.setOldestAvailableOffset(String.valueOf(p.earliestOffset));
                    topicPartition.setNewestAvailableOffset(String.valueOf(p.latestOffset));
                    return topicPartition;
                })
                .collect(toList());

        final List<TopicPartition> actual = kafkaRepository.listPartitions(topic);

        assertThat(actual, containsInAnyOrder(expected.toArray()));
    }

    @Test
    public void validateValidCursors() {
        final List<Cursor> topic1Cursors = asList(cursor("0", "41"), cursor("1", "100"), cursor("1", "200"), cursor("1", "101"));
        for (final Cursor topic1Cursor : topic1Cursors) {
            assertThat(topic1Cursor.toString(), kafkaRepository.areCursorsValid(MY_TOPIC, asList(topic1Cursor)), is(true));
        }
        assertThat(kafkaRepository.areCursorsValid(MY_TOPIC, topic1Cursors), is(true));

        final List<Cursor> topic2Cursors = asList(cursor("1", "0"), cursor("1", "100"), cursor("5", "12"), cursor("9", "100"));
        for (final Cursor topic2Cursor : topic2Cursors) {
            assertThat(topic2Cursor.toString(), kafkaRepository.areCursorsValid(ANOTHER_TOPIC, asList(topic2Cursor)), is(true));
        }
        assertThat(kafkaRepository.areCursorsValid(ANOTHER_TOPIC, topic2Cursors), is(true));
    }

    private Cursor cursor(final String partition, final String offset) {
        return new Cursor(partition, offset);
    }

    private KafkaRepository createKafkaRepository() {
        final Consumer consumer = mock(Consumer.class);

        PARTITIONS.stream().map(p -> p.topic).distinct().forEach(
                topic -> when(consumer.partitionsFor(topic)).thenReturn(partitionsOfTopic(topic)));

        final SimpleConsumer simpleConsumer = mock(SimpleConsumer.class);
        when(simpleConsumer.getOffsetsBefore(any(OffsetRequest.class)))
            .thenAnswer(new OffsetResponseAnswer(PARTITIONS));

        final KafkaFactory kafkaFactory = mock(KafkaFactory.class);

        when(kafkaFactory.getSimpleConsumer()).thenReturn(simpleConsumer);
        when(kafkaFactory.getConsumer()).thenReturn(consumer);

        return new KafkaRepository(null, kafkaFactory, null);
    }

    private List<PartitionInfo> partitionsOfTopic(final String topic) {
        return PARTITIONS.stream()
                .filter(p -> p.topic.equals(topic))
                .map(p -> partitionInfo(p.topic, p.partition))
                .collect(toList());
    }

    private org.apache.kafka.common.PartitionInfo partitionInfo(final String topic, final int partition) {
        return new org.apache.kafka.common.PartitionInfo(topic, partition, null, null, null);
    }

}
