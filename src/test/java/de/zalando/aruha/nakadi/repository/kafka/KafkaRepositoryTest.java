package de.zalando.aruha.nakadi.repository.kafka;

import de.zalando.aruha.nakadi.NakadiException;
import de.zalando.aruha.nakadi.domain.Cursor;
import de.zalando.aruha.nakadi.domain.Topic;
import de.zalando.aruha.nakadi.domain.TopicPartition;
import de.zalando.aruha.nakadi.repository.zookeeper.ZooKeeperHolder;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.GetChildrenBuilder;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class KafkaRepositoryTest {

    public static final String MY_TOPIC = "my-topic";
    public static final String ANOTHER_TOPIC = "another-topic";

    public static final ProducerRecord EXPECTED_PRODUCER_RECORD = new ProducerRecord(MY_TOPIC, 0, "0", "payload");

    private static final Set<PartitionState> PARTITIONS;

    static {
        PARTITIONS = new HashSet<>();

        PARTITIONS.add(new PartitionState(MY_TOPIC, 0, 40, 42));
        PARTITIONS.add(new PartitionState(MY_TOPIC, 1, 100, 200));

        PARTITIONS.add(new PartitionState(ANOTHER_TOPIC, 1, 0, 100));
        PARTITIONS.add(new PartitionState(ANOTHER_TOPIC, 5, 12, 60));
        PARTITIONS.add(new PartitionState(ANOTHER_TOPIC, 9, 99, 222));
    }

    public static final List<Cursor> MY_TOPIC_VALID_CURSORS = asList(cursor("0", "41"), cursor("1", "100"), cursor("1", "200"), cursor("1", "101"));
    public static final List<Cursor> ANOTHER_TOPIC_CURSORS = asList(cursor("1", "0"), cursor("1", "100"), cursor("5", "12"), cursor("9", "100"));
    public static final List<Cursor> MY_TOPIC_INVALID_CURSORS = asList(cursor("0", "39"), cursor("1", "0"), cursor("1", "99"), cursor("1", "201"));

    public static final long LATEST_TIME = kafka.api.OffsetRequest.LatestTime();
    public static final long EARLIEST_TIME = kafka.api.OffsetRequest.EarliestTime();


    private final KafkaRepository kafkaRepository;
    private final KafkaProducer kafkaProducer;

    public KafkaRepositoryTest() {
        kafkaProducer = mock(KafkaProducer.class);
        kafkaRepository = createKafkaRepository();
    }


    @Test
    public void canListAllTopics() throws Exception {
        final List<Topic> allTopics = allTopics().stream().map(Topic::new).collect(toList());
        assertThat(kafkaRepository.listTopics(), containsInAnyOrder(allTopics.toArray()));
    }

    @Test
    public void canDetermineIfTopicExists() throws NakadiException {
        assertThat(kafkaRepository.topicExists(MY_TOPIC), is(true));
        assertThat(kafkaRepository.topicExists(ANOTHER_TOPIC), is(true));

        assertThat(kafkaRepository.topicExists("doesnt-exist"), is(false));
    }

    @Test
    public void validateValidCursors() {
        // validate each individual valid cursor
        for (final Cursor cursor : MY_TOPIC_VALID_CURSORS) {
            assertThat(cursor.toString(), kafkaRepository.areCursorsValid(MY_TOPIC, asList(cursor)), is(true));
        }
        // validate all valid cursors
        assertThat(kafkaRepository.areCursorsValid(MY_TOPIC, MY_TOPIC_VALID_CURSORS), is(true));

        // validate each individual valid cursor
        for (final Cursor cursor : ANOTHER_TOPIC_CURSORS) {
            assertThat(cursor.toString(), kafkaRepository.areCursorsValid(ANOTHER_TOPIC, asList(cursor)), is(true));
        }
        // validate all valid cursors
        assertThat(kafkaRepository.areCursorsValid(ANOTHER_TOPIC, ANOTHER_TOPIC_CURSORS), is(true));
    }

    @Test
    public void invalidateInvalidCursors() {
        for (final Cursor invalidCursor : MY_TOPIC_INVALID_CURSORS) {
            assertThat(invalidCursor.toString(), kafkaRepository.areCursorsValid(MY_TOPIC, asList(invalidCursor)), is(false));

            // check combination with valid cursor
            for (Cursor validCursor : MY_TOPIC_VALID_CURSORS) {
                assertThat(invalidCursor.toString(), kafkaRepository.areCursorsValid(MY_TOPIC, asList(validCursor, invalidCursor)), is(false));
            }
        }
        assertThat(kafkaRepository.areCursorsValid(MY_TOPIC, MY_TOPIC_INVALID_CURSORS), is(false));
    }

    @Test
    public void canPostAnEvent() throws Exception {
        kafkaRepository.postEvent(
                EXPECTED_PRODUCER_RECORD.topic(),
                String.valueOf(EXPECTED_PRODUCER_RECORD.partition()),
                (String) EXPECTED_PRODUCER_RECORD.value());

        verify(kafkaProducer, times(1)).send(EXPECTED_PRODUCER_RECORD);
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

    private static Cursor cursor(final String partition, final String offset) {
        return new Cursor(partition, offset);
    }

    private KafkaRepository createKafkaRepository() {
        try {
            return new KafkaRepository(createZooKeeperHolder(), createKafkaFactory(), createKafkaRepositorySettings());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private KafkaRepositorySettings createKafkaRepositorySettings() {
        final KafkaRepositorySettings settings = mock(KafkaRepositorySettings.class);
        return settings;
    }

    private ZooKeeperHolder createZooKeeperHolder() throws Exception {
        // GetChildrenBuilder
        final GetChildrenBuilder getChildrenBuilder = mock(GetChildrenBuilder.class);
        when(getChildrenBuilder.forPath("/brokers/topics")).thenReturn(allTopics());

        // Curator Framework
        final CuratorFramework curatorFramework = mock(CuratorFramework.class);
        when(curatorFramework.getChildren()).thenReturn(getChildrenBuilder);

        // ZooKeeperHolder
        final ZooKeeperHolder zkHolder = mock(ZooKeeperHolder.class);
        when(zkHolder.get()).thenReturn(curatorFramework);

        return zkHolder;
    }

    private static List<String> allTopics() {
        return PARTITIONS.stream().map(p -> p.topic).distinct().collect(toList());
    }

    private KafkaFactory createKafkaFactory() {
        // Consumer
        final Consumer consumer = mock(Consumer.class);

        allTopics().stream().forEach(
                topic -> when(consumer.partitionsFor(topic)).thenReturn(partitionsOfTopic(topic)));

        // SimpleConsumer
        final SimpleConsumer simpleConsumer = mock(SimpleConsumer.class);
        when(simpleConsumer.getOffsetsBefore(any(OffsetRequest.class)))
                .thenAnswer(new OffsetResponseAnswer(PARTITIONS));

        // KafkaProducer
        when(kafkaProducer.send(EXPECTED_PRODUCER_RECORD)).thenReturn(mock(Future.class));

        // KafkaFactory
        final KafkaFactory kafkaFactory = mock(KafkaFactory.class);

        when(kafkaFactory.getSimpleConsumer()).thenReturn(simpleConsumer);
        when(kafkaFactory.getConsumer()).thenReturn(consumer);
        when(kafkaFactory.createProducer()).thenReturn(kafkaProducer);

        return kafkaFactory;
    }

    private List<PartitionInfo> partitionsOfTopic(final String topic) {
        return PARTITIONS.stream()
                .filter(p -> p.topic.equals(topic))
                .map(p -> partitionInfo(p.topic, p.partition))
                .collect(toList());
    }

    private static org.apache.kafka.common.PartitionInfo partitionInfo(final String topic, final int partition) {
        return new org.apache.kafka.common.PartitionInfo(topic, partition, null, null, null);
    }

}
