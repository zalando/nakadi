package de.zalando.aruha.nakadi.repository.kafka;

import static de.zalando.aruha.nakadi.domain.TopicPartition.topicPartition;

import static org.hamcrest.MatcherAssert.assertThat;

import static org.hamcrest.Matchers.equalTo;

import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static de.zalando.aruha.nakadi.utils.TestUtils.randomString;
import static de.zalando.aruha.nakadi.utils.TestUtils.randomUInt;
import static de.zalando.aruha.nakadi.utils.TestUtils.randomUIntAsString;
import static de.zalando.aruha.nakadi.utils.TestUtils.randomULong;
import static de.zalando.aruha.nakadi.utils.TestUtils.randomULongAsString;

import static junit.framework.TestCase.fail;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import de.zalando.aruha.nakadi.domain.Cursor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.NoOffsetForPartitionException;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;

import org.junit.Test;

import org.mockito.ArgumentCaptor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import de.zalando.aruha.nakadi.NakadiException;
import de.zalando.aruha.nakadi.domain.ConsumedEvent;

public class NakadiKafkaConsumerTest {

    private static final String TOPIC = randomString();
    private static final int PARTITION = randomUInt();
    private static final long POLL_TIMEOUT = randomULong();

    @Test
    @SuppressWarnings("unchecked")
    public void whenCreateNakadiConsumerThenKafkaConsumerConfiguredCorrectly() {

        // ARRANGE //
        final KafkaConsumer<String, String> kafkaConsumerMock = mock(KafkaConsumer.class);

        Class<List<TopicPartition>> topicPartitionListClass = (Class<List<TopicPartition>>) (Class) List.class;
        final ArgumentCaptor<List<TopicPartition>> partitionsCaptor = ArgumentCaptor.forClass(topicPartitionListClass);
        doNothing().when(kafkaConsumerMock).assign(partitionsCaptor.capture());

        final ArgumentCaptor<TopicPartition> tpCaptor = ArgumentCaptor.forClass(TopicPartition.class);
        final ArgumentCaptor<Long> offsetCaptor = ArgumentCaptor.forClass(Long.class);
        doNothing().when(kafkaConsumerMock).seek(tpCaptor.capture(), offsetCaptor.capture());

        final KafkaFactory kafkaFactoryMock = createKafkaFactoryMock(kafkaConsumerMock);

        final List<Cursor> cursors = ImmutableList.of(
                new Cursor(randomString(), randomUIntAsString(), randomULongAsString()),
                new Cursor(randomString(), randomUIntAsString(), randomULongAsString()),
                new Cursor(randomString(), randomUIntAsString(), randomULongAsString()));

        // ACT //
        new NakadiKafkaConsumer(kafkaFactoryMock, cursors, POLL_TIMEOUT);

        // ASSERT //
        final List<TopicPartition> expectedTPs = cursors
                .stream()
                .map(cursor -> new TopicPartition(cursor.getTopic(), Integer.parseInt(cursor.getPartition())))
                .collect(Collectors.toList());

        final List<TopicPartition> assignedPartitions = partitionsCaptor.getValue();
        assertThat(assignedPartitions, equalTo(expectedTPs));

        final List<TopicPartition> topicPartitions = tpCaptor.getAllValues();
        assertThat(topicPartitions, equalTo(expectedTPs));

        final List<Long> offsets = offsetCaptor.getAllValues();
        assertThat(offsets, equalTo(cursors
                .stream()
                .map(x -> Long.parseLong(x.getOffset()))
                .collect(Collectors.toList())));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void whenReadEventsThenGetRightEvents() throws NakadiException {

        // ARRANGE //
        final String event1 = randomString();
        final String event2 = randomString();
        final int event1Offset = randomUInt();
        final int event2Offset = randomUInt();
        final ConsumerRecords<String, String> consumerRecords = new ConsumerRecords<>(ImmutableMap.of(
                    new TopicPartition(TOPIC, PARTITION),
                    ImmutableList.of(new ConsumerRecord<>(TOPIC, PARTITION, event1Offset, "k1", event1),
                        new ConsumerRecord<>(TOPIC, PARTITION, event2Offset, "k2", event2))));
        final ConsumerRecords<String, String> emptyRecords = new ConsumerRecords<>(ImmutableMap.of());

        final KafkaConsumer<String, String> kafkaConsumerMock = mock(KafkaConsumer.class);
        final ArgumentCaptor<Long> pollTimeoutCaptor = ArgumentCaptor.forClass(Long.class);
        when(kafkaConsumerMock.poll(pollTimeoutCaptor.capture())).thenReturn(consumerRecords, emptyRecords);

        final KafkaFactory kafkaFactoryMock = createKafkaFactoryMock(kafkaConsumerMock);

        // we mock KafkaConsumer anyway, so the cursors we pass are not really important
        final ImmutableList<Cursor> cursors = ImmutableList.of(new Cursor(TOPIC, Integer.toString(PARTITION), "0"));

        // ACT //
        final NakadiKafkaConsumer consumer = new NakadiKafkaConsumer(kafkaFactoryMock, cursors, POLL_TIMEOUT);

        final Optional<ConsumedEvent> consumedEvent1 = consumer.readEvent();
        final Optional<ConsumedEvent> consumedEvent2 = consumer.readEvent();
        final Optional<ConsumedEvent> consumedEvent3 = consumer.readEvent();

        // ASSERT //
        assertThat("The event we read first should not be empty", consumedEvent1.isPresent(), equalTo(true));
        assertThat("The event we read first should have the same data as first mocked ConsumerRecord",
            consumedEvent1.get(),
            equalTo(new ConsumedEvent(event1, topicPartition(TOPIC, Integer.toString(PARTITION)), Integer.toString(event1Offset + 1))));

        assertThat("The event we read second should not be empty", consumedEvent2.isPresent(), equalTo(true));
        assertThat("The event we read second should have the same data as second mocked ConsumerRecord",
            consumedEvent2.get(),
            equalTo(new ConsumedEvent(event2, topicPartition(TOPIC, Integer.toString(PARTITION)), Integer.toString(event2Offset + 1))));

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

            final KafkaFactory kafkaFactoryMock = createKafkaFactoryMock(kafkaConsumerMock);
            try {

                // ACT //
                final NakadiKafkaConsumer consumer = new NakadiKafkaConsumer(kafkaFactoryMock,
                        ImmutableList.of(new Cursor(TOPIC, Integer.toString(PARTITION), "0")), POLL_TIMEOUT);
                consumer.readEvent();

                // ASSERT //
                fail("The NakadiException should be thrown");
            } catch (NakadiException e) {
                numberOfNakadiExceptions++;
            }
        }

        assertThat("We should get a NakadiException for every call", numberOfNakadiExceptions,
            equalTo(exceptions.size()));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void whenFetchNextOffsetsThenOk() throws NakadiException {

        // ARRANGE //
        final List<de.zalando.aruha.nakadi.domain.TopicPartition> partitions = ImmutableList.of(
                topicPartition(TOPIC, randomUIntAsString()),
                topicPartition(TOPIC, randomUIntAsString()),
                topicPartition(TOPIC, randomUIntAsString()));
        final List<Cursor> cursors = partitions
                .stream()
                .map(tp -> new Cursor(tp.getTopic(), tp.getPartition(), randomULongAsString()))
                .collect(Collectors.toList());

        final KafkaConsumer<String, String> kafkaConsumerMock = mock(KafkaConsumer.class);
        final Map<de.zalando.aruha.nakadi.domain.TopicPartition, String> expectedOffsets = Maps.newHashMap();
        partitions.forEach(tp -> {
            final Long nextOffset = randomULong();
            when(kafkaConsumerMock.position(new TopicPartition(tp.getTopic(), Integer.valueOf(tp.getPartition()))))
                    .thenReturn(nextOffset);
            expectedOffsets.put(tp, Long.toString(nextOffset));
        });

        final KafkaFactory kafkaFactoryMock = createKafkaFactoryMock(kafkaConsumerMock);

        // ACT //
        final NakadiKafkaConsumer consumer = new NakadiKafkaConsumer(kafkaFactoryMock, cursors, POLL_TIMEOUT);
        final Map<de.zalando.aruha.nakadi.domain.TopicPartition, String> offsets = consumer.fetchNextOffsets();

        // ASSERT //
        assertThat("We should get offsets according to what we provided in kafkaConsumerMock", offsets,
            equalTo(expectedOffsets));
    }

    private KafkaFactory createKafkaFactoryMock(final KafkaConsumer<String, String> kafkaConsumerMock) {
        final KafkaFactory kafkaFactoryMock = mock(KafkaFactory.class);
        when(kafkaFactoryMock.getConsumer()).thenReturn(kafkaConsumerMock);
        return kafkaFactoryMock;
    }

}
