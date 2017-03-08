package org.zalando.nakadi.repository.kafka;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.GetChildrenBuilder;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.BufferExhaustedException;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.errors.TimeoutException;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.zalando.nakadi.config.NakadiSettings;
import org.zalando.nakadi.domain.BatchItem;
import org.zalando.nakadi.domain.CursorError;
import org.zalando.nakadi.domain.EventPublishingStatus;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.domain.SubscriptionBase;
import org.zalando.nakadi.exceptions.EventPublishingException;
import org.zalando.nakadi.exceptions.InvalidCursorException;
import org.zalando.nakadi.exceptions.NakadiException;
import org.zalando.nakadi.repository.zookeeper.ZooKeeperHolder;
import org.zalando.nakadi.repository.zookeeper.ZookeeperSettings;
import org.zalando.nakadi.util.UUIDGenerator;
import org.zalando.nakadi.utils.EventTypeTestBuilder;
import org.zalando.nakadi.utils.RandomSubscriptionBuilder;
import org.zalando.nakadi.view.Cursor;
import org.zalando.nakadi.view.SubscriptionCursorWithoutToken;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;

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
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class KafkaTopicRepositoryTest {

    public static final String MY_TOPIC = "my-topic";
    public static final String ANOTHER_TOPIC = "another-topic";
    private final NakadiSettings nakadiSettings = mock(NakadiSettings.class);
    private final KafkaSettings kafkaSettings = mock(KafkaSettings.class);
    private final ZookeeperSettings zookeeperSettings = mock(ZookeeperSettings.class);
    private static final String KAFKA_CLIENT_ID = "application_name-topic_name";
    private static final Meter METER = new MetricRegistry().meter("some-meter");

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
            cursor("1", "150")); // something in the middle

    public static final List<Cursor> ANOTHER_TOPIC_VALID_CURSORS = asList(cursor("1", "0"), cursor("1", "99"),
            cursor("5", "30"), cursor("9", "100"));

    private final KafkaTopicRepository kafkaTopicRepository;
    private final KafkaProducer<String, String> kafkaProducer;
    private final KafkaFactory kafkaFactory;

    @SuppressWarnings("unchecked")
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
        final List<String> allTopics = allTopics().stream().collect(toList());
        assertThat(kafkaTopicRepository.listTopics(), containsInAnyOrder(allTopics.toArray()));
    }

    @Test
    public void canDetermineIfTopicExists() throws NakadiException {
        assertThat(kafkaTopicRepository.topicExists(MY_TOPIC), is(true));
        assertThat(kafkaTopicRepository.topicExists(ANOTHER_TOPIC), is(true));

        assertThat(kafkaTopicRepository.topicExists("doesnt-exist"), is(false));
    }

    private static List<NakadiCursor> asTopicPosition(final String topic, final List<Cursor> cursors) {
        return cursors.stream()
                .map(c -> new NakadiCursor(topic, c.getPartition(), c.getOffset()))
                .collect(toList());
    }

    @Test
    @SuppressWarnings("ArraysAsListWithZeroOrOneArgument")
    public void validateValidCursors() throws NakadiException, InvalidCursorException {
        // validate each individual valid cursor
        for (final Cursor cursor : MY_TOPIC_VALID_CURSORS) {
            kafkaTopicRepository.createEventConsumer(KAFKA_CLIENT_ID, asTopicPosition(MY_TOPIC, asList(cursor)));
        }
        // validate all valid cursors
        kafkaTopicRepository.createEventConsumer(KAFKA_CLIENT_ID, asTopicPosition(MY_TOPIC, MY_TOPIC_VALID_CURSORS));

        // validate each individual valid cursor
        for (final Cursor cursor : ANOTHER_TOPIC_VALID_CURSORS) {
            kafkaTopicRepository.createEventConsumer(KAFKA_CLIENT_ID, asTopicPosition(ANOTHER_TOPIC, asList(cursor)));
        }
        // validate all valid cursors
        kafkaTopicRepository.createEventConsumer(
                KAFKA_CLIENT_ID, asTopicPosition(ANOTHER_TOPIC, ANOTHER_TOPIC_VALID_CURSORS));
    }

    @Test
    @SuppressWarnings("ArraysAsListWithZeroOrOneArgument")
    public void invalidateInvalidCursors() throws NakadiException {
        final Cursor outOfBoundOffset = cursor("0", "38");
        try {
            kafkaTopicRepository.createEventConsumer(
                    KAFKA_CLIENT_ID, asTopicPosition(MY_TOPIC, asList(outOfBoundOffset)));
        } catch (final InvalidCursorException e) {
            assertThat(e.getError(), equalTo(CursorError.UNAVAILABLE));
        }

        final Cursor nonExistingPartition = cursor("99", "100");
        try {
            kafkaTopicRepository.createEventConsumer(
                    KAFKA_CLIENT_ID, asTopicPosition(MY_TOPIC, asList(nonExistingPartition)));
        } catch (final InvalidCursorException e) {
            assertThat(e.getError(), equalTo(CursorError.PARTITION_NOT_FOUND));
        }

        final Cursor wrongOffset = cursor("0", "blah");
        try {
            kafkaTopicRepository.createEventConsumer(KAFKA_CLIENT_ID, asTopicPosition(MY_TOPIC, asList(wrongOffset)));
        } catch (final InvalidCursorException e) {
            assertThat(e.getError(), equalTo(CursorError.INVALID_FORMAT));
        }

    }

    @Test
    public void whenPostEventTimesOutThenUpdateItemStatus() throws Exception {
        final BatchItem item = new BatchItem("{}");
        item.setPartition("1");
        final List<BatchItem> batch = new ArrayList<>();
        batch.add(item);

        when(kafkaProducer.partitionsFor(EXPECTED_PRODUCER_RECORD.topic())).thenReturn(ImmutableList.of(
                new PartitionInfo(EXPECTED_PRODUCER_RECORD.topic(), 1, new Node(1, "host", 9091), null, null)));
        when(nakadiSettings.getKafkaSendTimeoutMs()).thenReturn((long) 100);
        Mockito
                .doReturn(mock(Future.class))
                .when(kafkaProducer)
                .send(any(), any());

        try {
            kafkaTopicRepository.syncPostBatch(EXPECTED_PRODUCER_RECORD.topic(), batch, METER);
            fail();
        } catch (final EventPublishingException e) {
            assertThat(item.getResponse().getPublishingStatus(), equalTo(EventPublishingStatus.FAILED));
            assertThat(item.getResponse().getDetail(), equalTo("timed out"));
        }
    }

    @Test
    public void whenPostEventOverflowsBufferThenUpdateItemStatus() throws Exception {
        final BatchItem item = new BatchItem("{}");
        item.setPartition("1");
        final List<BatchItem> batch = new ArrayList<>();
        batch.add(item);

        when(kafkaProducer.partitionsFor(EXPECTED_PRODUCER_RECORD.topic())).thenReturn(ImmutableList.of(
                new PartitionInfo(EXPECTED_PRODUCER_RECORD.topic(), 1, new Node(1, "host", 9091), null, null)));

        Mockito
                .doThrow(BufferExhaustedException.class)
                .when(kafkaProducer)
                .send(any(), any());

        try {
            kafkaTopicRepository.syncPostBatch(EXPECTED_PRODUCER_RECORD.topic(), batch, METER);
            fail();
        } catch (final EventPublishingException e) {
            assertThat(item.getResponse().getPublishingStatus(), equalTo(EventPublishingStatus.FAILED));
            assertThat(item.getResponse().getDetail(), equalTo("internal error"));
        }
    }

    @Test
    public void whenKafkaPublishCallbackWithExceptionThenEventPublishingException() throws Exception {

        final BatchItem firstItem = new BatchItem("{}");
        firstItem.setPartition("1");
        final BatchItem secondItem = new BatchItem("{}");
        secondItem.setPartition("2");
        final List<BatchItem> batch = ImmutableList.of(firstItem, secondItem);

        when(kafkaProducer.partitionsFor(EXPECTED_PRODUCER_RECORD.topic())).thenReturn(ImmutableList.of(
                new PartitionInfo(EXPECTED_PRODUCER_RECORD.topic(), 1, new Node(1, "host", 9091), null, null),
                new PartitionInfo(EXPECTED_PRODUCER_RECORD.topic(), 2, new Node(1, "host", 9091), null, null)));

        when(kafkaProducer.send(any(), any())).thenAnswer(invocation -> {
            final ProducerRecord record = (ProducerRecord) invocation.getArguments()[0];
            final Callback callback = (Callback) invocation.getArguments()[1];
            if (record.partition() == 2) {
                callback.onCompletion(null, new Exception()); // return exception only for second event
            } else {
                callback.onCompletion(null, null);
            }
            return null;
        });

        try {
            kafkaTopicRepository.syncPostBatch(EXPECTED_PRODUCER_RECORD.topic(), batch, METER);
            fail();
        } catch (final EventPublishingException e) {
            assertThat(firstItem.getResponse().getPublishingStatus(), equalTo(EventPublishingStatus.SUBMITTED));
            assertThat(firstItem.getResponse().getDetail(), equalTo(""));
            assertThat(secondItem.getResponse().getPublishingStatus(), equalTo(EventPublishingStatus.FAILED));
            assertThat(secondItem.getResponse().getDetail(), equalTo("internal error"));
        }
    }

    @Test
    public void whenValidateCommitCursorsThenOk() throws InvalidCursorException {
        kafkaTopicRepository.validateCommitCursor(new NakadiCursor(MY_TOPIC, "0", "23"));
    }

    @Test
    public void whenValidateInvalidCommitCursorsThenException() throws NakadiException {
        ImmutableMap.of(
                cursor("345", "1"), CursorError.PARTITION_NOT_FOUND,
                cursor("0", "abc"), CursorError.INVALID_FORMAT)
                .entrySet()
                .forEach(testCase -> {
                    try {
                        kafkaTopicRepository.validateCommitCursor(new NakadiCursor(
                                MY_TOPIC, testCase.getKey().getPartition(), testCase.getKey().getOffset()));
                    } catch (final InvalidCursorException e) {
                        assertThat(e.getError(), equalTo(testCase.getValue()));
                    }
                });
    }

    @Test
    public void whenMaterializePositionsFromBeginThenOk() throws Exception {
        final EventType eventType = EventTypeTestBuilder.builder()
                .topic(MY_TOPIC)
                .name("et")
                .build();
        final Subscription subscription = RandomSubscriptionBuilder.builder()
                .withEventType(eventType.getName())
                .withStartFrom(SubscriptionBase.InitialPosition.BEGIN)
                .build();

        final Map<String, Long> positions = kafkaTopicRepository.materializePositions(eventType, subscription);

        assertThat(positions, equalTo(ImmutableMap.of(
                "0", 40L,
                "1", 100L,
                "2", 0L
        )));
    }

    @Test
    public void whenMaterializePositionsFromEndThenOk() throws Exception {
        final EventType eventType = EventTypeTestBuilder.builder()
                .topic(MY_TOPIC)
                .name("et")
                .build();
        final Subscription subscription = RandomSubscriptionBuilder.builder()
                .withEventType(eventType.getName())
                .withStartFrom(SubscriptionBase.InitialPosition.END)
                .build();

        final Map<String, Long> positions = kafkaTopicRepository.materializePositions(eventType, subscription);

        assertThat(positions, equalTo(ImmutableMap.of(
                "0", 42L,
                "1", 200L,
                "2", 0L
        )));
    }

    @Test
    public void whenMaterializePositionsFromCursorsThenOk() throws Exception {
        final EventType eventType = EventTypeTestBuilder.builder()
                .topic(MY_TOPIC)
                .name("et")
                .build();
        final Subscription subscription = RandomSubscriptionBuilder.builder()
                .withEventType(eventType.getName())
                .withStartFrom(SubscriptionBase.InitialPosition.CURSORS)
                .withInitialCursors(ImmutableList.of(
                        new SubscriptionCursorWithoutToken("et", "0", "41"),
                        new SubscriptionCursorWithoutToken("et", "1", "124"),
                        new SubscriptionCursorWithoutToken("et", "2", Cursor.BEFORE_OLDEST_OFFSET)
                ))
                .build();

        final Map<String, Long> positions = kafkaTopicRepository.materializePositions(eventType, subscription);

        assertThat(positions, equalTo(ImmutableMap.of(
                "0", 42L,
                "1", 125L,
                "2", 0L
        )));
    }

    @Test
    public void whenKafkaPublishTimeoutThenCircuitIsOpened() throws Exception {

        when(nakadiSettings.getKafkaSendTimeoutMs()).thenReturn(1000L);

        when(kafkaProducer.partitionsFor(EXPECTED_PRODUCER_RECORD.topic())).thenReturn(ImmutableList.of(
                new PartitionInfo(EXPECTED_PRODUCER_RECORD.topic(), 1, new Node(1, "host", 9091), null, null)));

        when(kafkaProducer.send(any(), any())).thenAnswer(invocation -> {
            final Callback callback = (Callback) invocation.getArguments()[1];
            callback.onCompletion(null, new TimeoutException());
            return null;
        });

        final List<BatchItem> batches = new LinkedList<>();
        for (int i = 0; i < 1000; i++) {
            try {
                final BatchItem batchItem = new BatchItem("{}");
                batchItem.setPartition("1");
                batches.add(batchItem);
                kafkaTopicRepository.syncPostBatch(EXPECTED_PRODUCER_RECORD.topic(), ImmutableList.of(batchItem),
                        METER);
                fail();
            } catch (final EventPublishingException e) {
            }
        }

        Assert.assertTrue(batches.stream()
                .filter(item -> item.getResponse().getPublishingStatus() == EventPublishingStatus.FAILED &&
                        item.getResponse().getDetail().equals("short circuited"))
                .count() >= 1);
    }

    private static Cursor cursor(final String partition, final String offset) {
        return new Cursor(partition, offset);
    }

    private KafkaTopicRepository createKafkaRepository(final KafkaFactory kafkaFactory) {
        try {
            return new KafkaTopicRepository(createZooKeeperHolder(),
                    kafkaFactory,
                    nakadiSettings,
                    kafkaSettings,
                    zookeeperSettings,
                    new UUIDGenerator());
        } catch (final Exception e) {
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

        allTopics().forEach(
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

        when(kafkaFactory.getConsumer(KAFKA_CLIENT_ID)).thenReturn(consumer);
        when(kafkaFactory.getConsumer()).thenReturn(consumer);
        when(kafkaFactory.takeProducer()).thenReturn(kafkaProducer);

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
