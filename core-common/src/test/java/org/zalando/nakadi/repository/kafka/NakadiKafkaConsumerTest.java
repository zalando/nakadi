package org.zalando.nakadi.repository.kafka;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.NoOffsetForPartitionException;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.repository.LowLevelConsumer;
import org.zalando.nakadi.utils.TestUtils;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static junit.framework.TestCase.fail;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.zalando.nakadi.repository.kafka.KafkaCursor.toKafkaOffset;
import static org.zalando.nakadi.repository.kafka.KafkaCursor.toKafkaPartition;
import static org.zalando.nakadi.repository.kafka.KafkaCursor.toNakadiOffset;
import static org.zalando.nakadi.repository.kafka.KafkaCursor.toNakadiPartition;
import static org.zalando.nakadi.utils.TestUtils.buildTimeline;
import static org.zalando.nakadi.utils.TestUtils.randomString;
import static org.zalando.nakadi.utils.TestUtils.randomUInt;
import static org.zalando.nakadi.utils.TestUtils.randomULong;

public class NakadiKafkaConsumerTest {

    private static final String TOPIC = TestUtils.randomValidEventTypeName();
    private static final int PARTITION = randomUInt();
    private static final long POLL_TIMEOUT = randomULong();
    private static final Date CREATED_AT = new Date();

    private static KafkaCursor kafkaCursor(final String topic, final int partition, final long offset) {
        return new KafkaCursor(topic, partition, offset);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void whenCreateConsumerThenKafkaConsumerConfiguredCorrectly() {

        // ARRANGE //
        final KafkaConsumer<byte[], byte[]> kafkaConsumerMock = mock(KafkaConsumer.class);

        final Class<List<TopicPartition>> topicPartitionListClass = (Class) List.class;
        final ArgumentCaptor<List<TopicPartition>> partitionsCaptor = ArgumentCaptor.forClass(topicPartitionListClass);
        doNothing().when(kafkaConsumerMock).assign(partitionsCaptor.capture());

        final ArgumentCaptor<TopicPartition> tpCaptor = ArgumentCaptor.forClass(TopicPartition.class);
        final ArgumentCaptor<Long> offsetCaptor = ArgumentCaptor.forClass(Long.class);
        doNothing().when(kafkaConsumerMock).seek(tpCaptor.capture(), offsetCaptor.capture());

        final List<KafkaCursor> kafkaCursors = ImmutableList.of(
                kafkaCursor(TOPIC, randomUInt(), randomUInt()),
                kafkaCursor(TOPIC, randomUInt(), randomUInt()),
                kafkaCursor(TOPIC, randomUInt(), randomUInt()));

        // ACT //
        final Map<String, String> cursors = kafkaCursors.stream().collect(Collectors.toMap(kafkaCursor ->
                        toNakadiPartition(kafkaCursor.getPartition()),
                kafkaCursor -> toNakadiOffset(kafkaCursor.getOffset())));
        final Timeline timeline = buildTimeline(TOPIC, TOPIC, CREATED_AT);
        final List<NakadiCursor> nakadiCursors = kafkaCursors.stream()
                .map(kafkaCursor -> kafkaCursor.toNakadiCursor(timeline)).collect(Collectors.toList());

        final NakadiKafkaConsumer consumer =
                new NakadiKafkaConsumer(kafkaConsumerMock, POLL_TIMEOUT);
        consumer.reassign(nakadiCursors);

        // ASSERT //
        final List<TopicPartition> assignedPartitions = partitionsCaptor.getValue();
        assertThat(assignedPartitions, hasSize(cursors.size()));
        assignedPartitions.forEach(partition -> {
            assertThat(partition.topic(), equalTo(TOPIC));
            assertThat(cursors.keySet(), Matchers.hasItem((Integer.toString(partition.partition()))));
        });

        final List<TopicPartition> topicPartitions = tpCaptor.getAllValues();
        final List<Long> offsets = offsetCaptor.getAllValues();
        assertThat(topicPartitions, hasSize(cursors.size()));
        assertThat(offsets, hasSize(cursors.size()));
        cursors
                .entrySet().forEach(cursor -> {
                    assertThat(topicPartitions,
                            Matchers.hasItem(new TopicPartition(TOPIC, toKafkaPartition(cursor.getKey()))));
                    assertThat(offsets, Matchers.hasItem(toKafkaOffset(cursor.getValue()) + 1));
                });
    }

    @Test
    @SuppressWarnings("unchecked")
    public void whenReadEventsThenGetRightEvents() {

        // ARRANGE //
        final byte[] event1 = randomString().getBytes();
        final byte[] event2 = randomString().getBytes();
        final int event1Offset = randomUInt();
        final int event2Offset = randomUInt();
        final long now = System.currentTimeMillis();
        final ConsumerRecords<byte[], byte[]> consumerRecords = new ConsumerRecords<>(ImmutableMap.of(
                new TopicPartition(TOPIC, PARTITION),
                ImmutableList.of(
                        createRecord(TOPIC, PARTITION, event1Offset, now, "k1".getBytes(), event1),
                        createRecord(TOPIC, PARTITION, event2Offset, now, "k2".getBytes(), event2))));
        final Timeline timeline = buildTimeline(TOPIC, TOPIC, CREATED_AT);
        final ConsumerRecords<byte[], byte[]> emptyRecords = new ConsumerRecords<>(ImmutableMap.of());

        final KafkaConsumer<byte[], byte[]> kafkaConsumerMock = mock(KafkaConsumer.class);
        final ArgumentCaptor<Long> pollTimeoutCaptor = ArgumentCaptor.forClass(Long.class);
        when(kafkaConsumerMock.poll(pollTimeoutCaptor.capture())).thenReturn(consumerRecords, emptyRecords);

        // we mock KafkaConsumer anyway, so the cursors we pass are not really important
        final List<KafkaCursor> cursors = ImmutableList.of(kafkaCursor(TOPIC, PARTITION, 0));

        // ACT //
        final List<NakadiCursor> nakadiCursors = cursors.stream()
                .map(kafkaCursor -> kafkaCursor.toNakadiCursor(timeline)).collect(Collectors.toList());

        final NakadiKafkaConsumer consumer =
                new NakadiKafkaConsumer(kafkaConsumerMock, POLL_TIMEOUT);
        consumer.reassign(nakadiCursors);
        final List<LowLevelConsumer.Event> consumedEvents = consumer.readEvents();

        // ASSERT //
        assertThat("The event we read first should not be empty", consumedEvents.size(), equalTo(2));
        assertThat("The event we read first should have the same data as first mocked ConsumerRecord",
                consumedEvents.get(0),
                equalTo(new LowLevelConsumer.Event(
                        event1, TOPIC, PARTITION, event1Offset, now, null, null)));

        assertThat("The event we read second should have the same data as second mocked ConsumerRecord",
                consumedEvents.get(1),
                equalTo(new LowLevelConsumer.Event(
                        event2, TOPIC, PARTITION, event2Offset, now, null, null)));

        assertThat("The kafka poll should be called with timeout we defined", pollTimeoutCaptor.getValue(),
                equalTo(POLL_TIMEOUT));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void whenReadEventsThenNakadiRuntimeBaseException() {

        // ARRANGE //
        final ImmutableList<RuntimeException> exceptions = ImmutableList.of(new NoOffsetForPartitionException(
                new TopicPartition("", 0)), new KafkaException());

        int numberOfNakadiRuntimeBaseExceptions = 0;
        for (final Exception exception : exceptions) {
            final KafkaConsumer<byte[], byte[]> kafkaConsumerMock = mock(KafkaConsumer.class);
            when(kafkaConsumerMock.poll(POLL_TIMEOUT)).thenThrow(exception);

            try {

                // ACT //
                final NakadiKafkaConsumer consumer = new NakadiKafkaConsumer(kafkaConsumerMock, POLL_TIMEOUT);
                consumer.readEvents();

                // ASSERT //
                fail("An Exception was expected to be be thrown");
            } catch (final Exception e) {
                numberOfNakadiRuntimeBaseExceptions++;
            }
        }

        assertThat("We should get a NakadiBaseException for every call",
                numberOfNakadiRuntimeBaseExceptions,
                equalTo(exceptions.size()));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void whenCloseThenKafkaConsumerIsClosed() {
        // ARRANGE //
        final KafkaConsumer<byte[], byte[]> kafkaConsumerMock = mock(KafkaConsumer.class);
        final NakadiKafkaConsumer nakadiKafkaConsumer = new NakadiKafkaConsumer(kafkaConsumerMock, POLL_TIMEOUT);
        // ACT //
        nakadiKafkaConsumer.close();
        // ASSERT //
        verify(kafkaConsumerMock, times(1)).close();
    }

    private ConsumerRecord createRecord(final String topic,
                                        final int partition,
                                        final long offset,
                                        final long timestamp,
                                        final byte[] key,
                                        final byte[] value) {
        return new ConsumerRecord(topic, partition, offset, timestamp,
                TimestampType.CREATE_TIME, 0, 0, key, value, new RecordHeaders(), Optional.empty());
    }

}
