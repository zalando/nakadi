package de.zalando.aruha.nakadi.repository.kafka;

import static org.hamcrest.MatcherAssert.assertThat;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static de.zalando.aruha.nakadi.repository.kafka.KafkaCursor.kafkaCursor;
import static de.zalando.aruha.nakadi.repository.kafka.KafkaCursor.toNakadiOffset;
import static de.zalando.aruha.nakadi.repository.kafka.KafkaCursor.toNakadiPartition;
import static de.zalando.aruha.nakadi.utils.TestUtils.randomString;
import static de.zalando.aruha.nakadi.utils.TestUtils.randomUInt;
import static de.zalando.aruha.nakadi.utils.TestUtils.randomULong;

import static junit.framework.TestCase.fail;

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

import org.junit.Test;

import org.mockito.ArgumentCaptor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import de.zalando.aruha.nakadi.domain.ConsumedEvent;
import de.zalando.aruha.nakadi.utils.TestUtils;

public class NakadiKafkaConsumerTest {

    private static final String TOPIC = TestUtils.randomValidEventTypeName();
    private static final int PARTITION = randomUInt();
    private static final long POLL_TIMEOUT = randomULong();

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

        final List<KafkaCursor> kafkaCursors = ImmutableList.of(kafkaCursor(randomUInt(), randomULong()),
                kafkaCursor(randomUInt(), randomULong()), kafkaCursor(randomUInt(), randomULong()));

        // ACT //
        new NakadiKafkaConsumer(kafkaConsumerMock, TOPIC, kafkaCursors, POLL_TIMEOUT);

        // ASSERT //
        final Map<String, String> cursors = kafkaCursors.stream().collect(Collectors.toMap(kafkaCursor ->
                        toNakadiPartition(kafkaCursor.getPartition()),
                    kafkaCursor -> toNakadiOffset(kafkaCursor.getOffset())));

        final List<TopicPartition> assignedPartitions = partitionsCaptor.getValue();
        assertThat(assignedPartitions, hasSize(cursors.size()));
        assignedPartitions.forEach(partition -> {
            assertThat(partition.topic(), equalTo(TOPIC));
            assertThat(cursors.keySet().contains((Integer.toString(partition.partition()))), is(true));
        });

        final List<TopicPartition> topicPartitions = tpCaptor.getAllValues();
        final List<Long> offsets = offsetCaptor.getAllValues();
        assertThat(topicPartitions, hasSize(cursors.size()));
        assertThat(offsets, hasSize(cursors.size()));
        cursors
            .entrySet().stream().forEach(cursor -> {
                assertThat(topicPartitions.contains(new TopicPartition(TOPIC, Integer.parseInt(cursor.getKey()))),
                    is(true));
                assertThat(offsets.contains(Long.parseLong(cursor.getValue())), is(true));
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
        final ConsumerRecords<String, String> emptyRecords = new ConsumerRecords<>(ImmutableMap.of());

        final KafkaConsumer<String, String> kafkaConsumerMock = mock(KafkaConsumer.class);
        final ArgumentCaptor<Long> pollTimeoutCaptor = ArgumentCaptor.forClass(Long.class);
        when(kafkaConsumerMock.poll(pollTimeoutCaptor.capture())).thenReturn(consumerRecords, emptyRecords);

        // we mock KafkaConsumer anyway, so the cursors we pass are not really important
        final List<KafkaCursor> cursors = ImmutableList.of(kafkaCursor(PARTITION, 0));

        // ACT //
        final NakadiKafkaConsumer consumer = new NakadiKafkaConsumer(kafkaConsumerMock, TOPIC, cursors, POLL_TIMEOUT);
        final Optional<ConsumedEvent> consumedEvent1 = consumer.readEvent();
        final Optional<ConsumedEvent> consumedEvent2 = consumer.readEvent();
        final Optional<ConsumedEvent> consumedEvent3 = consumer.readEvent();

        // ASSERT //
        assertThat("The event we read first should not be empty", consumedEvent1.isPresent(), equalTo(true));
        assertThat("The event we read first should have the same data as first mocked ConsumerRecord",
            consumedEvent1.get(),
            equalTo(new ConsumedEvent(event1, TOPIC, Integer.toString(PARTITION), Integer.toString(event1Offset))));

        assertThat("The event we read second should not be empty", consumedEvent2.isPresent(), equalTo(true));
        assertThat("The event we read second should have the same data as second mocked ConsumerRecord",
            consumedEvent2.get(),
            equalTo(new ConsumedEvent(event2, TOPIC, Integer.toString(PARTITION), Integer.toString(event2Offset))));

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
                final NakadiKafkaConsumer consumer = new NakadiKafkaConsumer(kafkaConsumerMock, TOPIC,
                        ImmutableList.of(), POLL_TIMEOUT);
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

}
