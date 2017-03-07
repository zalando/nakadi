package org.zalando.nakadi.repository.kafka;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.NoOffsetForPartitionException;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.zalando.nakadi.domain.ConsumedEvent;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.utils.TestUtils;
import static junit.framework.TestCase.fail;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.zalando.nakadi.repository.kafka.KafkaCursor.toKafkaOffset;
import static org.zalando.nakadi.repository.kafka.KafkaCursor.toKafkaPartition;
import static org.zalando.nakadi.repository.kafka.KafkaCursor.toNakadiOffset;
import static org.zalando.nakadi.repository.kafka.KafkaCursor.toNakadiPartition;
import static org.zalando.nakadi.utils.TestUtils.createFakeTimeline;
import static org.zalando.nakadi.utils.TestUtils.randomString;
import static org.zalando.nakadi.utils.TestUtils.randomUInt;
import static org.zalando.nakadi.utils.TestUtils.randomULong;

public class NakadiKafkaConsumerTest {

    private static final String TOPIC = TestUtils.randomValidEventTypeName();
    private static final int PARTITION = randomUInt();
    private static final long POLL_TIMEOUT = randomULong();

    private static KafkaCursor kafkaCursor(final String topic, final int partition, final long offset) {
        return new KafkaCursor(topic, partition, offset);
    }

    private static Map<TopicPartition, Timeline> createTpTimelineMap() {
        final Timeline timeline = createFakeTimeline(TOPIC);
        final Map<TopicPartition, Timeline> mockMap = mock(Map.class);
        when(mockMap.get(any())).thenReturn(timeline);
        return mockMap;
    }

    @Test
    @SuppressWarnings("unchecked")
    public void whenCreateConsumerThenKafkaConsumerConfiguredCorrectly() {

        // ARRANGE //
        final KafkaConsumer<String, String> kafkaConsumerMock = mock(KafkaConsumer.class);

        final Class<List<TopicPartition>> topicPartitionListClass = (Class) List.class;
        final ArgumentCaptor<List<TopicPartition>> partitionsCaptor = ArgumentCaptor.forClass(topicPartitionListClass);
        doNothing().when(kafkaConsumerMock).assign(partitionsCaptor.capture());

        final ArgumentCaptor<TopicPartition> tpCaptor = ArgumentCaptor.forClass(TopicPartition.class);
        final ArgumentCaptor<Long> offsetCaptor = ArgumentCaptor.forClass(Long.class);
        doNothing().when(kafkaConsumerMock).seek(tpCaptor.capture(), offsetCaptor.capture());

        final List<KafkaCursor> kafkaCursors = ImmutableList.of(
                kafkaCursor(TOPIC, randomUInt(), randomULong()),
                kafkaCursor(TOPIC, randomUInt(), randomULong()),
                kafkaCursor(TOPIC, randomUInt(), randomULong()));

        // ACT //
        new NakadiKafkaConsumer(kafkaConsumerMock, kafkaCursors, createTpTimelineMap(), POLL_TIMEOUT);

        // ASSERT //
        final Map<String, String> cursors = kafkaCursors.stream().collect(Collectors.toMap(kafkaCursor ->
                        toNakadiPartition(kafkaCursor.getPartition()),
                kafkaCursor -> toNakadiOffset(kafkaCursor.getOffset())));

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
            assertThat(offsets, Matchers.hasItem(toKafkaOffset(cursor.getValue())));
        });
    }

    @Test
    @SuppressWarnings("unchecked")
    public void whenReadEventsThenGetRightEvents() {

        // ARRANGE //
        final String event1 = randomString();
        final String event2 = randomString();
        final int event1Offset = randomUInt();
        final int event2Offset = randomUInt();
        final ConsumerRecords<String, String> consumerRecords = new ConsumerRecords<>(ImmutableMap.of(
                new TopicPartition(TOPIC, PARTITION),
                ImmutableList.of(new ConsumerRecord<>(TOPIC, PARTITION, event1Offset, "k1", event1),
                        new ConsumerRecord<>(TOPIC, PARTITION, event2Offset, "k2", event2))));
        final Timeline timeline = createFakeTimeline(TOPIC);
        final ConsumerRecords<String, String> emptyRecords = new ConsumerRecords<>(ImmutableMap.of());

        final KafkaConsumer<String, String> kafkaConsumerMock = mock(KafkaConsumer.class);
        final ArgumentCaptor<Long> pollTimeoutCaptor = ArgumentCaptor.forClass(Long.class);
        when(kafkaConsumerMock.poll(pollTimeoutCaptor.capture())).thenReturn(consumerRecords, emptyRecords);

        // we mock KafkaConsumer anyway, so the cursors we pass are not really important
        final List<KafkaCursor> cursors = ImmutableList.of(kafkaCursor(TOPIC, PARTITION, 0));

        // ACT //
        final NakadiKafkaConsumer consumer = new NakadiKafkaConsumer(
                kafkaConsumerMock, cursors, createTpTimelineMap(), POLL_TIMEOUT);
        final Optional<ConsumedEvent> consumedEvent1 = consumer.readEvent();
        final Optional<ConsumedEvent> consumedEvent2 = consumer.readEvent();
        final Optional<ConsumedEvent> consumedEvent3 = consumer.readEvent();

        // ASSERT //
        assertThat("The event we read first should not be empty", consumedEvent1.isPresent(), equalTo(true));
        assertThat("The event we read first should have the same data as first mocked ConsumerRecord",
                consumedEvent1.get(),
                equalTo(new ConsumedEvent(event1,
                        new KafkaCursor(TOPIC, PARTITION, event1Offset).toNakadiCursor(timeline))));

        assertThat("The event we read second should not be empty", consumedEvent2.isPresent(), equalTo(true));
        assertThat("The event we read second should have the same data as second mocked ConsumerRecord",
                consumedEvent2.get(),
                equalTo(new ConsumedEvent(event2,
                        new KafkaCursor(TOPIC, PARTITION, event2Offset).toNakadiCursor(timeline))));

        assertThat("The event we read third should be empty", consumedEvent3.isPresent(), equalTo(false));

        assertThat("The kafka poll should be called with timeout we defined", pollTimeoutCaptor.getValue(),
                equalTo(POLL_TIMEOUT));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void whenReadEventsThenNakadiException() {

        // ARRANGE //
        final ImmutableList<RuntimeException> exceptions = ImmutableList.of(new NoOffsetForPartitionException(
                new TopicPartition("", 0)), new KafkaException());

        int numberOfNakadiExceptions = 0;
        for (final Exception exception : exceptions) {
            final KafkaConsumer<String, String> kafkaConsumerMock = mock(KafkaConsumer.class);
            when(kafkaConsumerMock.poll(POLL_TIMEOUT)).thenThrow(exception);

            try {

                // ACT //
                final NakadiKafkaConsumer consumer = new NakadiKafkaConsumer(kafkaConsumerMock,
                        ImmutableList.of(), createTpTimelineMap(), POLL_TIMEOUT);
                consumer.readEvent();

                // ASSERT //
                fail("An Exception was expected to be be thrown");
            } catch (final Exception e) {
                numberOfNakadiExceptions++;
            }
        }

        assertThat("We should get a NakadiException for every call", numberOfNakadiExceptions,
                equalTo(exceptions.size()));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void whenCloseThenKafkaConsumerIsClosed() {
        // ARRANGE //
        final KafkaConsumer<String, String> kafkaConsumerMock = mock(KafkaConsumer.class);
        final NakadiKafkaConsumer nakadiKafkaConsumer = new NakadiKafkaConsumer(kafkaConsumerMock,
                ImmutableList.of(), createTpTimelineMap(), POLL_TIMEOUT);
        // ACT //
        nakadiKafkaConsumer.close();
        // ASSERT //
        verify(kafkaConsumerMock, times(1)).close();
    }

}
