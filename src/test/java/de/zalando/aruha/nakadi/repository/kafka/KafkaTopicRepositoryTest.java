package de.zalando.aruha.nakadi.repository.kafka;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import de.zalando.aruha.nakadi.domain.BatchItem;
import de.zalando.aruha.nakadi.domain.Cursor;
import de.zalando.aruha.nakadi.domain.EventPublishingStatus;
import de.zalando.aruha.nakadi.domain.Topic;
import de.zalando.aruha.nakadi.domain.TopicPartition;
import de.zalando.aruha.nakadi.exceptions.EventPublishingException;
import de.zalando.aruha.nakadi.exceptions.NakadiException;
import de.zalando.aruha.nakadi.repository.zookeeper.ZooKeeperHolder;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.GetChildrenBuilder;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.BufferExhaustedException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.json.JSONObject;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.function.Function;

import static de.zalando.aruha.nakadi.repository.kafka.KafkaCursor.kafkaCursor;
import static java.lang.String.valueOf;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.anyVararg;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class KafkaTopicRepositoryTest {

    public static final String MY_TOPIC = "my-topic";
    public static final String ANOTHER_TOPIC = "another-topic";
    final KafkaRepositorySettings settings = mock(KafkaRepositorySettings.class);

    @SuppressWarnings("unchecked")
    public static final ProducerRecord EXPECTED_PRODUCER_RECORD = new ProducerRecord(MY_TOPIC, 0, "0", "payload");

    private static final Set<PartitionState> PARTITIONS;

    static {
        PARTITIONS = new HashSet<>();

        PARTITIONS.add(new PartitionState(MY_TOPIC, 0, 40, 42));
        PARTITIONS.add(new PartitionState(MY_TOPIC, 1, 100, 200));
        PARTITIONS.add(new PartitionState(MY_TOPIC, 2, 0, 0));

        PARTITIONS.add(new PartitionState(ANOTHER_TOPIC, 1, 0, 100));
        PARTITIONS.add(new PartitionState(ANOTHER_TOPIC, 5, 12, 60));
        PARTITIONS.add(new PartitionState(ANOTHER_TOPIC, 9, 99, 222));
    }

    private ConsumerOffsetMode offsetMode = ConsumerOffsetMode.EARLIEST;
    private enum ConsumerOffsetMode {
        EARLIEST,
        LATEST
    }

    public static final List<Cursor> MY_TOPIC_VALID_CURSORS = asList(
            cursor("0", "39"), // the first one possible
            cursor("0", "40"), // something in the middle
            cursor("0", "41"), // the last one possible
            cursor("1", "150"), // something in the middle
            cursor("1", Cursor.BEFORE_OLDEST_OFFSET)); // consume from the very beginning

    public static final List<Cursor> ANOTHER_TOPIC_VALID_CURSORS = asList(cursor("1", "0"), cursor("1", "99"),
            cursor("5", "30"), cursor("9", "100"));

    public static final List<Cursor> MY_TOPIC_INVALID_CURSORS = asList(
            cursor("0", "38"),   // out of bounds
            cursor("0", "42"),   // out of bounds
            cursor("0", "blah"), // wrong offset
            cursor("1", "98"),   // out of bounds
            cursor("1", "200"),  // out of bounds
            cursor("2", "0"),    // out of bounds for partition with no events ever
            cursor("99", "100")); // none existing partition

    private static final List<String> MY_TOPIC_VALID_PARTITIONS = ImmutableList.of("0", "1", "2");
    private static final List<String> MY_TOPIC_INVALID_PARTITIONS = ImmutableList.of("3", "-1", "abc");

    private static final Function<PartitionState, TopicPartition> PARTITION_STATE_TO_TOPIC_PARTITION = p -> {
        final TopicPartition topicPartition = new TopicPartition(p.topic, valueOf(p.partition));
        topicPartition.setOldestAvailableOffset(valueOf(p.earliestOffset));
        final String newestAvailable = p.latestOffset == 0 ? Cursor.BEFORE_OLDEST_OFFSET : valueOf(p.latestOffset - 1);
        topicPartition.setNewestAvailableOffset(newestAvailable);
        return topicPartition;
    };

    private final KafkaTopicRepository kafkaTopicRepository;
    private final KafkaProducer kafkaProducer;
    private final KafkaFactory kafkaFactory;

    public KafkaTopicRepositoryTest() {
        kafkaProducer = mock(KafkaProducer.class);
        when(kafkaProducer.partitionsFor(anyString())).then(
                invocation -> partitionsOfTopic((String) invocation.getArguments()[0])
        );

        kafkaFactory = createKafkaFactory();
        kafkaTopicRepository = createKafkaRepository(kafkaFactory);
    }


    @Test
    public void canListAllTopics() throws Exception {
        final List<Topic> allTopics = allTopics().stream().map(Topic::new).collect(toList());
        assertThat(kafkaTopicRepository.listTopics(), containsInAnyOrder(allTopics.toArray()));
    }

    @Test
    public void canDetermineIfTopicExists() throws NakadiException {
        assertThat(kafkaTopicRepository.topicExists(MY_TOPIC), is(true));
        assertThat(kafkaTopicRepository.topicExists(ANOTHER_TOPIC), is(true));

        assertThat(kafkaTopicRepository.topicExists("doesnt-exist"), is(false));
    }

    @Test
    public void canDetermineIfPartitionExists() throws NakadiException {
        for (final String validPartition : MY_TOPIC_VALID_PARTITIONS) {
            assertThat(kafkaTopicRepository.partitionExists(MY_TOPIC, validPartition), is(true));
        }
        for (final String validPartition : MY_TOPIC_INVALID_PARTITIONS) {
            assertThat(kafkaTopicRepository.partitionExists(MY_TOPIC, validPartition), is(false));
        }
    }

    @Test
    @SuppressWarnings("ArraysAsListWithZeroOrOneArgument")
    public void validateValidCursors() throws NakadiException {
        // validate each individual valid cursor
        for (final Cursor cursor : MY_TOPIC_VALID_CURSORS) {
            assertThat(cursor.toString(), kafkaTopicRepository.areCursorsValid(MY_TOPIC, asList(cursor)), is(true));
        }
        // validate all valid cursors
        assertThat(kafkaTopicRepository.areCursorsValid(MY_TOPIC, MY_TOPIC_VALID_CURSORS), is(true));

        // validate each individual valid cursor
        for (final Cursor cursor : ANOTHER_TOPIC_VALID_CURSORS) {
            assertThat(cursor.toString(), kafkaTopicRepository.areCursorsValid(ANOTHER_TOPIC, asList(cursor)), is(true));
        }
        // validate all valid cursors
        assertThat(kafkaTopicRepository.areCursorsValid(ANOTHER_TOPIC, ANOTHER_TOPIC_VALID_CURSORS), is(true));
    }

    @Test
    @SuppressWarnings("ArraysAsListWithZeroOrOneArgument")
    public void invalidateInvalidCursors() throws NakadiException {
        for (final Cursor invalidCursor : MY_TOPIC_INVALID_CURSORS) {
            assertThat(invalidCursor.toString(), kafkaTopicRepository.areCursorsValid(MY_TOPIC, asList(invalidCursor)), is(false));

            // check combination with valid cursor
            for (Cursor validCursor : MY_TOPIC_VALID_CURSORS) {
                assertThat(invalidCursor.toString(), kafkaTopicRepository.areCursorsValid(MY_TOPIC, asList(validCursor, invalidCursor)), is(false));
            }
        }
        assertThat(kafkaTopicRepository.areCursorsValid(MY_TOPIC, MY_TOPIC_INVALID_CURSORS), is(false));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void canPostAnEvent() throws Exception {
        kafkaTopicRepository.postEvent(
                EXPECTED_PRODUCER_RECORD.topic(),
                valueOf(EXPECTED_PRODUCER_RECORD.partition()),
                (String) EXPECTED_PRODUCER_RECORD.value());

        verify(kafkaProducer, times(1)).send(EXPECTED_PRODUCER_RECORD);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void whenPostEventTimesOutThenUpdateItemStatus() throws Exception {
        final BatchItem item = new BatchItem(new JSONObject());
        item.setPartition("1");
        final List<BatchItem> batch = new ArrayList<>();
        batch.add(item);

        when(settings.getKafkaSendTimeoutMs()).thenReturn((long) 100);

        Mockito
                .doReturn(mock(Future.class))
                .when(kafkaProducer)
                .send(any(), any());

        try {
            kafkaTopicRepository.syncPostBatch(EXPECTED_PRODUCER_RECORD.topic(), batch);
            fail();
        } catch (EventPublishingException e) {
            assertThat(item.getResponse().getPublishingStatus(), equalTo(EventPublishingStatus.FAILED));
            assertThat(item.getResponse().getDetail(), equalTo("timed out"));
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void whenPostEventOverflowsBufferThenUpdateItemStatus() throws Exception {
        final BatchItem item = new BatchItem(new JSONObject());
        item.setPartition("1");
        final List<BatchItem> batch = new ArrayList<>();
        batch.add(item);

        Mockito
                .doThrow(BufferExhaustedException.class)
                .when(kafkaProducer)
                .send(any(), any());

        try {
            kafkaTopicRepository.syncPostBatch(EXPECTED_PRODUCER_RECORD.topic(), batch);
            fail();
        } catch (EventPublishingException e) {
            assertThat(item.getResponse().getPublishingStatus(), equalTo(EventPublishingStatus.FAILED));
            assertThat(item.getResponse().getDetail(), equalTo("internal error"));
        }
    }

    @Test
    public void canListAllPartitions() throws NakadiException {
        canListAllPartitionsOfTopic(MY_TOPIC);
        canListAllPartitionsOfTopic(ANOTHER_TOPIC);
    }

    @Test
    public void canGetPartition() throws NakadiException {
        PARTITIONS
                .stream()
                .map(PARTITION_STATE_TO_TOPIC_PARTITION)
                .forEach(tp -> {
                    try {
                        final TopicPartition actual = kafkaTopicRepository.getPartition(tp.getTopicId(), tp.getPartitionId());
                        assertThat(actual, equalTo(tp));
                    } catch (NakadiException e) {
                        fail("Should not get NakadiException for this call");
                    }
                });
    }

    @Test
    @SuppressWarnings("unchecked")
    public void canCreateEventConsumerWithOffsetsTransformed() throws NakadiException {
        // ACT /
        final Map<String, String> cursors = ImmutableMap.of(
                "0", "23",
                "1", Cursor.BEFORE_OLDEST_OFFSET);
        kafkaTopicRepository.createEventConsumer(MY_TOPIC, cursors);

        // ASSERT //
        Class<List<KafkaCursor>> kafkaCursorListClass = (Class<List<KafkaCursor>>) (Class) List.class;
        final ArgumentCaptor<List<KafkaCursor>> captor = ArgumentCaptor.forClass(kafkaCursorListClass);
        verify(kafkaFactory).createNakadiConsumer(eq(MY_TOPIC), captor.capture(), eq(0L));

        final List<KafkaCursor> kafkaCursors = captor.getValue();
        assertThat(kafkaCursors, equalTo(ImmutableList.of(
                kafkaCursor(0, 24),
                kafkaCursor(1, 100)
        )));
    }

    private void canListAllPartitionsOfTopic(final String topic) throws NakadiException {
        final List<TopicPartition> expected = PARTITIONS
                .stream()
                .filter(p -> p.topic.equals(topic))
                .map(PARTITION_STATE_TO_TOPIC_PARTITION)
                .collect(toList());

        final List<TopicPartition> actual = kafkaTopicRepository.listPartitions(topic);

        assertThat(actual, containsInAnyOrder(expected.toArray()));
    }

    private static Cursor cursor(final String partition, final String offset) {
        return new Cursor(partition, offset);
    }

    private KafkaTopicRepository createKafkaRepository(final KafkaFactory kafkaFactory) {
        try {
            return new KafkaTopicRepository(createZooKeeperHolder(), kafkaFactory, settings);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
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

    @SuppressWarnings("unchecked")
    private KafkaFactory createKafkaFactory() {
        // Consumer
        final Consumer consumer = mock(Consumer.class);

        allTopics().stream().forEach(
                topic -> when(consumer.partitionsFor(topic)).thenReturn(partitionsOfTopic(topic)));

        doAnswer(invocation -> {
            offsetMode = ConsumerOffsetMode.EARLIEST;
            return null;
        }).when(consumer).seekToBeginning(anyVararg());

        doAnswer(invocation -> {
            offsetMode = ConsumerOffsetMode.LATEST;
            return null;
        }).when(consumer).seekToEnd(anyVararg());

        when(consumer.position(any())).thenAnswer(invocation -> {
            final org.apache.kafka.common.TopicPartition tp =
                    (org.apache.kafka.common.TopicPartition) invocation.getArguments()[0];
            return PARTITIONS
                    .stream()
                    .filter(ps -> ps.topic.equals(tp.topic()) && ps.partition == tp.partition())
                    .findFirst()
                    .map(ps -> offsetMode == ConsumerOffsetMode.LATEST ? ps.latestOffset : ps.earliestOffset)
                    .orElseThrow(KafkaException::new);
        });

        // KafkaProducer
        when(kafkaProducer.send(EXPECTED_PRODUCER_RECORD)).thenReturn(mock(Future.class));

        // KafkaFactory
        final KafkaFactory kafkaFactory = mock(KafkaFactory.class);

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

    private static PartitionInfo partitionInfo(final String topic, final int partition) {
        return new PartitionInfo(topic, partition, null, null, null);
    }

}
