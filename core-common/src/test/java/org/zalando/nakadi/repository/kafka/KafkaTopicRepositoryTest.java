package org.zalando.nakadi.repository.kafka;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableList;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.BufferExhaustedException;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.header.Header;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.zalando.nakadi.config.NakadiSettings;
import org.zalando.nakadi.domain.BatchItem;
import org.zalando.nakadi.domain.CursorError;
import org.zalando.nakadi.domain.EventOwnerHeader;
import org.zalando.nakadi.domain.EventPublishingStatus;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.PartitionEndStatistics;
import org.zalando.nakadi.domain.PartitionStatistics;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.domain.TopicPartition;
import org.zalando.nakadi.exceptions.runtime.EventPublishingException;
import org.zalando.nakadi.exceptions.runtime.InvalidCursorException;
import org.zalando.nakadi.repository.zookeeper.ZookeeperSettings;
import org.zalando.nakadi.view.Cursor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.google.common.collect.Sets.newHashSet;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.anyVararg;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.zalando.nakadi.utils.TestUtils.buildTimelineWithTopic;

public class KafkaTopicRepositoryTest {

    public static final String MY_TOPIC = "my-topic";
    public static final String ANOTHER_TOPIC = "another-topic";
    private static final Node NODE = new Node(1, "host", 9091);
    private final NakadiSettings nakadiSettings = mock(NakadiSettings.class);
    private final KafkaSettings kafkaSettings = mock(KafkaSettings.class);
    private final ZookeeperSettings zookeeperSettings = mock(ZookeeperSettings.class);
    private final KafkaTopicConfigFactory kafkaTopicConfigFactory = mock(KafkaTopicConfigFactory.class);
    private final KafkaLocationManager kafkaLocationManager = mock(KafkaLocationManager.class);
    private static final String KAFKA_CLIENT_ID = "application_name-topic_name";

    @Captor
    private ArgumentCaptor<ProducerRecord<String, String>> producerRecordArgumentCaptor;

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
        //lower hystrix configs for tests to execute faster
        System.setProperty(
                String.format(
                        "hystrix.command.%s.metrics.healthSnapshot.intervalInMilliseconds",
                        NODE.idString() + "_" + NODE.host()),
                "10");
        System.setProperty(
                String.format(
                        "hystrix.command.%s.metrics.rollingStats.timeInMilliseconds",
                        NODE.idString() + "_" + NODE.host()),
                "500");
        System.setProperty(
                String.format(
                        "hystrix.command.%s.circuitBreaker.sleepWindowInMilliseconds",
                        NODE.idString() + "_" + NODE.host()),
                "500");

        kafkaProducer = mock(KafkaProducer.class);
        when(kafkaProducer.partitionsFor(anyString())).then(
                invocation -> partitionsOfTopic((String) invocation.getArguments()[0])
        );
        kafkaFactory = createKafkaFactory();
        kafkaTopicRepository = createKafkaRepository(kafkaFactory, new MetricRegistry());
        MockitoAnnotations.initMocks(this);
    }


    @Test
    public void canListAllTopics() {
        final List<String> allTopics = allTopics().stream().collect(toList());
        assertThat(kafkaTopicRepository.listTopics(), containsInAnyOrder(allTopics.toArray()));
    }

    @Test
    public void testRecordHeaderSetWhilePublishing() {
        final String myTopic = "event-owner-selector-events";
        final BatchItem item = new BatchItem("{}", null,
                null,
                Collections.emptyList());
        item.setPartition("1");
        item.setOwner(new EventOwnerHeader("retailer", "nakadi"));
        final List<BatchItem> batch = ImmutableList.of(item);

        when(kafkaProducer.partitionsFor(myTopic)).thenReturn(ImmutableList.of(
                new PartitionInfo(myTopic, 1, NODE, null, null)));

        try {
            kafkaTopicRepository.syncPostBatch(myTopic, batch, "random", false);
            fail();
        } catch (final EventPublishingException e) {
            final ProducerRecord<String, String> recordSent = captureProducerRecordSent();
            final Header nameHeader = recordSent.headers().headers(EventOwnerHeader.AUTH_PARAM_NAME)
                    .iterator().next();
            Assert.assertEquals(new String(nameHeader.value()), "retailer");
            final Header valueHeader = recordSent.headers().headers(EventOwnerHeader.AUTH_PARAM_VALUE)
                    .iterator().next();
            Assert.assertEquals(new String(valueHeader.value()), "nakadi");
        }
    }

    @Test
    public void canDetermineIfTopicExists() {
        assertThat(kafkaTopicRepository.topicExists(MY_TOPIC), is(true));
        assertThat(kafkaTopicRepository.topicExists(ANOTHER_TOPIC), is(true));

        assertThat(kafkaTopicRepository.topicExists("doesnt-exist"), is(false));
    }

    private static List<NakadiCursor> asTopicPosition(final String topic, final List<Cursor> cursors) {
        return cursors.stream()
                .map(c -> NakadiCursor.of(buildTimelineWithTopic(topic), c.getPartition(), c.getOffset()))
                .collect(toList());
    }

    @Test
    @SuppressWarnings("ArraysAsListWithZeroOrOneArgument")
    public void validateValidCursors() throws InvalidCursorException {
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
    public void invalidateInvalidCursors() {
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
    public void canLoadPartitionStatistics() {
        final Timeline t1 = mock(Timeline.class);
        when(t1.getTopic()).thenReturn(MY_TOPIC);
        final Timeline t2 = mock(Timeline.class);
        when(t2.getTopic()).thenReturn(ANOTHER_TOPIC);
        final ImmutableList<Timeline> timelines = ImmutableList.of(t1, t2);

        final List<PartitionStatistics> stats = kafkaTopicRepository.loadTopicStatistics(timelines);

        final Set<PartitionStatistics> expected = PARTITIONS.stream()
                .map(p -> {
                    final Timeline timeline = p.topic.equals(MY_TOPIC) ? t1 : t2;
                    return new KafkaPartitionStatistics(timeline, p.partition, p.earliestOffset, p.latestOffset - 1);
                })
                .collect(Collectors.toSet());
        assertThat(newHashSet(stats), equalTo(expected));
    }

    @Test
    public void canLoadPartitionEndStatistics() {
        final Timeline t1 = mock(Timeline.class);
        when(t1.getTopic()).thenReturn(MY_TOPIC);
        final Timeline t2 = mock(Timeline.class);
        when(t2.getTopic()).thenReturn(ANOTHER_TOPIC);
        final ImmutableList<Timeline> timelines = ImmutableList.of(t1, t2);

        final List<PartitionEndStatistics> stats = kafkaTopicRepository.loadTopicEndStatistics(timelines);

        final Set<PartitionEndStatistics> expected = PARTITIONS.stream()
                .map(p -> {
                    final Timeline timeline = p.topic.equals(MY_TOPIC) ? t1 : t2;
                    return new KafkaPartitionEndStatistics(timeline, p.partition, p.latestOffset - 1);
                })
                .collect(Collectors.toSet());
        assertThat(newHashSet(stats), equalTo(expected));
    }

    @Test
    public void whenPostEventTimesOutThenUpdateItemStatus() {
        final BatchItem item = new BatchItem(
                "{}",
                BatchItem.EmptyInjectionConfiguration.build(1, true),
                new BatchItem.InjectionConfiguration[BatchItem.Injection.values().length],
                Collections.emptyList());
        item.setPartition("1");
        final List<BatchItem> batch = new ArrayList<>();
        batch.add(item);

        when(kafkaProducer.partitionsFor(EXPECTED_PRODUCER_RECORD.topic())).thenReturn(ImmutableList.of(
                new PartitionInfo(EXPECTED_PRODUCER_RECORD.topic(), 1, NODE, null, null)));
        when(nakadiSettings.getKafkaSendTimeoutMs()).thenReturn((long) 100);
        Mockito
                .doReturn(mock(Future.class))
                .when(kafkaProducer)
                .send(any(), any());

        try {
            kafkaTopicRepository.syncPostBatch(EXPECTED_PRODUCER_RECORD.topic(), batch, "random", false);
            fail();
        } catch (final EventPublishingException e) {
            assertThat(item.getResponse().getPublishingStatus(), equalTo(EventPublishingStatus.FAILED));
            assertThat(item.getResponse().getDetail(), equalTo("timed out"));
        }
    }

    @Test
    public void whenPartitionLeaderNotFoundTheRestOfTheBatchOk() {
        final BatchItem firstItem = new BatchItem(
                "{}",
                BatchItem.EmptyInjectionConfiguration.build(1, true),
                new BatchItem.InjectionConfiguration[BatchItem.Injection.values().length],
                Collections.emptyList());
        firstItem.setPartition("1");

        final BatchItem secondItem = new BatchItem(
                "{}",
                BatchItem.EmptyInjectionConfiguration.build(1, true),
                new BatchItem.InjectionConfiguration[BatchItem.Injection.values().length],
                Collections.emptyList());
        secondItem.setPartition("2");

        final List<BatchItem> batch = new ArrayList<>();
        batch.add(firstItem);
        batch.add(secondItem);

        when(kafkaProducer.partitionsFor(EXPECTED_PRODUCER_RECORD.topic())).thenReturn(ImmutableList.of(
                new PartitionInfo(EXPECTED_PRODUCER_RECORD.topic(), 1, null, null, null),
                new PartitionInfo(EXPECTED_PRODUCER_RECORD.topic(), 2, NODE, null, null)));

        when(kafkaProducer.send(any(), any())).thenAnswer(invocation -> {
            final Callback callback = (Callback) invocation.getArguments()[1];
            callback.onCompletion(null, null);
            return null;
        });

        Assert.assertThrows(EventPublishingException.class, () -> {
            kafkaTopicRepository.syncPostBatch(EXPECTED_PRODUCER_RECORD.topic(), batch, "random", false);
        });

        assertThat(firstItem.getResponse().getPublishingStatus(), equalTo(EventPublishingStatus.FAILED));
        assertThat(firstItem.getResponse().getDetail(), containsString("No leader for partition"));

        assertThat(secondItem.getResponse().getPublishingStatus(), equalTo(EventPublishingStatus.SUBMITTED));
    }

    @Test
    public void whenPostEventOverflowsBufferThenUpdateItemStatus() {
        final BatchItem item = new BatchItem("{}",
                BatchItem.EmptyInjectionConfiguration.build(1, true),
                new BatchItem.InjectionConfiguration[BatchItem.Injection.values().length],
                Collections.emptyList());
        item.setPartition("1");
        final List<BatchItem> batch = new ArrayList<>();
        batch.add(item);

        when(kafkaProducer.partitionsFor(EXPECTED_PRODUCER_RECORD.topic())).thenReturn(ImmutableList.of(
                new PartitionInfo(EXPECTED_PRODUCER_RECORD.topic(), 1, NODE, null, null)));

        Mockito
                .doThrow(BufferExhaustedException.class)
                .when(kafkaProducer)
                .send(any(), any());

        try {
            kafkaTopicRepository.syncPostBatch(EXPECTED_PRODUCER_RECORD.topic(), batch, "random", false);
            fail();
        } catch (final EventPublishingException e) {
            assertThat(item.getResponse().getPublishingStatus(), equalTo(EventPublishingStatus.FAILED));
            assertThat(item.getResponse().getDetail(), equalTo("internal error"));
        }
    }

    @Test
    public void whenKafkaPublishCallbackWithExceptionThenEventPublishingException() {

        final BatchItem firstItem = new BatchItem("{}", BatchItem.EmptyInjectionConfiguration.build(1, true),
                new BatchItem.InjectionConfiguration[BatchItem.Injection.values().length],
                Collections.emptyList());
        firstItem.setPartition("1");
        final BatchItem secondItem = new BatchItem("{}", BatchItem.EmptyInjectionConfiguration.build(1, true),
                new BatchItem.InjectionConfiguration[BatchItem.Injection.values().length],
                Collections.emptyList());
        secondItem.setPartition("2");
        final List<BatchItem> batch = ImmutableList.of(firstItem, secondItem);

        when(kafkaProducer.partitionsFor(EXPECTED_PRODUCER_RECORD.topic())).thenReturn(ImmutableList.of(
                new PartitionInfo(EXPECTED_PRODUCER_RECORD.topic(), 1, NODE, null, null),
                new PartitionInfo(EXPECTED_PRODUCER_RECORD.topic(), 2, NODE, null, null)));

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
            kafkaTopicRepository.syncPostBatch(EXPECTED_PRODUCER_RECORD.topic(), batch, "random", false);
            fail();
        } catch (final EventPublishingException e) {
            assertThat(firstItem.getResponse().getPublishingStatus(), equalTo(EventPublishingStatus.SUBMITTED));
            assertThat(firstItem.getResponse().getDetail(), equalTo(""));
            assertThat(secondItem.getResponse().getPublishingStatus(), equalTo(EventPublishingStatus.FAILED));
            assertThat(secondItem.getResponse().getDetail(), equalTo("internal error"));
        }
    }

    @Test
    public void checkCircuitBreakerStateBasedOnKafkaResponse() {
        when(nakadiSettings.getKafkaSendTimeoutMs()).thenReturn(1000L);
        when(kafkaProducer.partitionsFor(EXPECTED_PRODUCER_RECORD.topic())).thenReturn(ImmutableList.of(
                new PartitionInfo(EXPECTED_PRODUCER_RECORD.topic(), 1, NODE, null, null)));

        //Timeout Exception should cause circuit breaker to open
        List<BatchItem> batches = setResponseForSendingBatches(new TimeoutException(), new MetricRegistry());
        Assert.assertTrue(batches.stream()
                .filter(item -> item.getResponse().getPublishingStatus() == EventPublishingStatus.FAILED &&
                        item.getResponse().getDetail().equals("short circuited"))
                .count() >= 1);

        //No exception should close the circuit
        batches = setResponseForSendingBatches(null, new MetricRegistry());
        Assert.assertTrue(batches.stream()
                .filter(item -> item.getResponse().getPublishingStatus() == EventPublishingStatus.SUBMITTED &&
                        item.getResponse().getDetail().equals(""))
                .count() >= 1);

        //Timeout Exception should cause circuit breaker to open again
        batches = setResponseForSendingBatches(new TimeoutException(), new MetricRegistry());
        Assert.assertTrue(batches.stream()
                .filter(item -> item.getResponse().getPublishingStatus() == EventPublishingStatus.FAILED &&
                        item.getResponse().getDetail().equals("short circuited"))
                .count() >= 1);

    }

    private List<BatchItem> setResponseForSendingBatches(final Exception e, final MetricRegistry metricRegistry) {
        when(kafkaProducer.send(any(), any())).thenAnswer(invocation -> {
            final Callback callback = (Callback) invocation.getArguments()[1];
            if (callback != null) {
                callback.onCompletion(null, e);
            }
            return null;
        });
        final List<BatchItem> batches = new LinkedList<>();
        final KafkaTopicRepository kafkaTopicRepository = createKafkaRepository(kafkaFactory, metricRegistry);
        for (int i = 0; i < 100; i++) {
            try {
                final BatchItem batchItem = new BatchItem("{}",
                        BatchItem.EmptyInjectionConfiguration.build(1, true),
                        new BatchItem.InjectionConfiguration[BatchItem.Injection.values().length],
                        Collections.emptyList());
                batchItem.setPartition("1");
                batches.add(batchItem);
                TimeUnit.MILLISECONDS.sleep(5);
                kafkaTopicRepository.syncPostBatch(EXPECTED_PRODUCER_RECORD.topic(),
                        ImmutableList.of(batchItem), "random", false);
            } catch (final EventPublishingException | InterruptedException ex) {
            }
        }
        return batches;
    }

    @Test
    public void testGetSizeStatsWorksProperly() throws Exception {
        final KafkaZookeeper kz = mock(KafkaZookeeper.class);
        when(kz.getBrokerIdsForSizeStats()).thenReturn(Arrays.asList("1", "2"));
        final Map<String, Map<String, Long>> stats1 = new HashMap<>();
        stats1.put("t1", new HashMap<>());
        stats1.get("t1").put("0", 1234L);
        stats1.get("t1").put("1", 321L);
        stats1.put("t2", new HashMap<>());
        stats1.get("t2").put("0", 111L);
        when(kz.getSizeStatsForBroker(eq("1"))).thenReturn(new BubukuSizeStats(null, stats1));
        final Map<String, Map<String, Long>> stats2 = new HashMap<>();
        stats2.put("t1", new HashMap<>());
        stats2.get("t1").put("0", 4321L);
        stats2.get("t1").put("1", 123L);
        stats2.put("t3", new HashMap<>());
        stats2.get("t3").put("0", 222L);
        when(kz.getSizeStatsForBroker(eq("2"))).thenReturn(new BubukuSizeStats(null, stats2));

        final KafkaTopicRepository ktr = new KafkaTopicRepository.Builder()
                .setKafkaZookeeper(kz)
                .build();

        final Map<TopicPartition, Long> result = ktr.getSizeStats();

        Assert.assertEquals(4, result.size());
        Assert.assertEquals(new Long(4321L), result.get(new TopicPartition("t1", "0")));
        Assert.assertEquals(new Long(321L), result.get(new TopicPartition("t1", "1")));
        Assert.assertEquals(new Long(111L), result.get(new TopicPartition("t2", "0")));
        Assert.assertEquals(new Long(222L), result.get(new TopicPartition("t3", "0")));
    }


    @Test
    public void whenPublishShortCircuitingIsRecorded() {
        when(nakadiSettings.getKafkaSendTimeoutMs()).thenReturn(1000L);
        when(kafkaProducer.partitionsFor(EXPECTED_PRODUCER_RECORD.topic())).thenReturn(ImmutableList.of(
                new PartitionInfo(EXPECTED_PRODUCER_RECORD.topic(), 1, new Node(1, "10.10.0.1", 9091), null, null)));

        final MetricRegistry metricRegistry = new MetricRegistry();
        setResponseForSendingBatches(new TimeoutException(), metricRegistry);
        final String meterName = metricRegistry.getMeters().firstKey();
        final Meter meter = metricRegistry.getMeters().get(meterName);
        Assert.assertEquals(meterName, "hystrix.short.circuit.1_10.10.0.1");
        Assert.assertTrue(meter.getCount() >= 1);
    }

    private static Cursor cursor(final String partition, final String offset) {
        return new Cursor(partition, offset);
    }

    private KafkaTopicRepository createKafkaRepository(final KafkaFactory kafkaFactory,
                                                       final MetricRegistry metricRegistry) {
        try {
            return new KafkaTopicRepository.Builder()
                    .setKafkaZookeeper(createKafkaZookeeper())
                    .setKafkaFactory(kafkaFactory)
                    .setNakadiSettings(nakadiSettings)
                    .setKafkaSettings(kafkaSettings)
                    .setZookeeperSettings(zookeeperSettings)
                    .setKafkaTopicConfigFactory(kafkaTopicConfigFactory)
                    .setKafkaLocationManager(kafkaLocationManager)
                    .setMetricRegistry(metricRegistry)
                    .build();
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    private KafkaZookeeper createKafkaZookeeper() throws Exception {
        final KafkaZookeeper result = mock(KafkaZookeeper.class);
        when(result.listTopics()).thenReturn(allTopics());
        return result;
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

    @SuppressWarnings("unchecked")
    private ProducerRecord<String, String> captureProducerRecordSent() {
        verify(kafkaProducer, atLeastOnce()).send(producerRecordArgumentCaptor.capture(), any());
        return producerRecordArgumentCaptor.getValue();
    }
}
