package de.zalando.aruha.nakadi.repository.kafka;

import static java.util.function.Function.identity;

import static org.hamcrest.MatcherAssert.assertThat;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

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
  public void whenCreateConsumerThenKafkaConsumerConfiguredCorrectly() {

    // ARRANGE //
    final KafkaConsumer<String, String> kafkaConsumerMock = mock(KafkaConsumer.class);

    Class<List<TopicPartition>> topicPartitionListClass =
        (Class<List<TopicPartition>>) (Class) List.class;
    final ArgumentCaptor<List<TopicPartition>> partitionsCaptor =
        ArgumentCaptor.forClass(topicPartitionListClass);
    doNothing().when(kafkaConsumerMock).assign(partitionsCaptor.capture());

    final ArgumentCaptor<TopicPartition> tpCaptor = ArgumentCaptor.forClass(TopicPartition.class);
    final ArgumentCaptor<Long> offsetCaptor = ArgumentCaptor.forClass(Long.class);
    doNothing().when(kafkaConsumerMock).seek(tpCaptor.capture(), offsetCaptor.capture());

    final KafkaFactory kafkaFactoryMock = createKafkaFactoryMock(kafkaConsumerMock);

    final ImmutableMap<String, String> cursors =
        ImmutableMap.of(
            randomUIntAsString(),
            randomULongAsString(),
            randomUIntAsString(),
            randomULongAsString(),
            randomUIntAsString(),
            randomULongAsString());

    // ACT //
    new NakadiKafkaConsumer(kafkaFactoryMock, TOPIC, cursors, POLL_TIMEOUT);

    // ASSERT //
    final List<TopicPartition> assignedPartitions = partitionsCaptor.getValue();
    assertThat(assignedPartitions, hasSize(cursors.size()));
    assignedPartitions.forEach(
        partition -> {
          assertThat(partition.topic(), equalTo(TOPIC));
          assertThat(
              cursors.keySet().contains((Integer.toString(partition.partition()))), is(true));
        });

    final List<TopicPartition> topicPartitions = tpCaptor.getAllValues();
    final List<Long> offsets = offsetCaptor.getAllValues();
    assertThat(topicPartitions, hasSize(cursors.size()));
    assertThat(offsets, hasSize(cursors.size()));
    cursors
        .entrySet()
        .stream()
        .forEach(
            cursor -> {
              assertThat(
                  topicPartitions.contains(
                      new TopicPartition(TOPIC, Integer.parseInt(cursor.getKey()))),
                  is(true));
              assertThat(offsets.contains(Long.parseLong(cursor.getValue())), is(true));
            });
  }

  @Test
  @SuppressWarnings("unchecked")
  public void whenReadEventsThenGetRightEvents() throws NakadiException {

    // ARRANGE //
    final String event1 = randomString();
    final String event2 = randomString();
    final int event1Offset = randomUInt();
    final int event2Offset = randomUInt();
    final ConsumerRecords<String, String> consumerRecords =
        new ConsumerRecords<>(
            ImmutableMap.of(
                new TopicPartition(TOPIC, PARTITION),
                ImmutableList.of(
                    new ConsumerRecord<>(TOPIC, PARTITION, event1Offset, "k1", event1),
                    new ConsumerRecord<>(TOPIC, PARTITION, event2Offset, "k2", event2))));
    final ConsumerRecords<String, String> emptyRecords = new ConsumerRecords<>(ImmutableMap.of());

    final KafkaConsumer<String, String> kafkaConsumerMock = mock(KafkaConsumer.class);
    final ArgumentCaptor<Long> pollTimeoutCaptor = ArgumentCaptor.forClass(Long.class);
    when(kafkaConsumerMock.poll(pollTimeoutCaptor.capture()))
        .thenReturn(consumerRecords, emptyRecords);

    final KafkaFactory kafkaFactoryMock = createKafkaFactoryMock(kafkaConsumerMock);

    // we mock KafkaConsumer anyway, so the cursors we pass are not really important
    final ImmutableMap<String, String> cursors = ImmutableMap.of(Integer.toString(PARTITION), "0");

    // ACT //
    final NakadiKafkaConsumer consumer =
        new NakadiKafkaConsumer(kafkaFactoryMock, TOPIC, cursors, POLL_TIMEOUT);
    final Optional<ConsumedEvent> consumedEvent1 = consumer.readEvent();
    final Optional<ConsumedEvent> consumedEvent2 = consumer.readEvent();
    final Optional<ConsumedEvent> consumedEvent3 = consumer.readEvent();

    // ASSERT //
    assertThat(
        "The event we read first should not be empty", consumedEvent1.isPresent(), equalTo(true));
    assertThat(
        "The event we read first should have the same data as first mocked ConsumerRecord",
        consumedEvent1.get(),
        equalTo(
            new ConsumedEvent(
                event1, TOPIC, Integer.toString(PARTITION), Integer.toString(event1Offset + 1))));

    assertThat(
        "The event we read second should not be empty", consumedEvent2.isPresent(), equalTo(true));
    assertThat(
        "The event we read second should have the same data as second mocked ConsumerRecord",
        consumedEvent2.get(),
        equalTo(
            new ConsumedEvent(
                event2, TOPIC, Integer.toString(PARTITION), Integer.toString(event2Offset + 1))));

    assertThat(
        "The event we read third should be empty", consumedEvent3.isPresent(), equalTo(false));

    assertThat(
        "The kafka poll should be called with timeout we defined",
        pollTimeoutCaptor.getValue(),
        equalTo(POLL_TIMEOUT));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void whenReadEventsThenNakadiException() {

    // ARRANGE //
    final ImmutableList<RuntimeException> exceptions =
        ImmutableList.of(
            new NoOffsetForPartitionException(new TopicPartition("", 0)), new KafkaException());

    int numberOfNakadiExceptions = 0;
    for (final Exception exception : exceptions) {
      final KafkaConsumer<String, String> kafkaConsumerMock = mock(KafkaConsumer.class);
      when(kafkaConsumerMock.poll(POLL_TIMEOUT)).thenThrow(exception);

      final KafkaFactory kafkaFactoryMock = createKafkaFactoryMock(kafkaConsumerMock);
      try {

        // ACT //
        final NakadiKafkaConsumer consumer =
            new NakadiKafkaConsumer(kafkaFactoryMock, TOPIC, ImmutableMap.of(), POLL_TIMEOUT);
        consumer.readEvent();

        // ASSERT //
        fail("The NakadiException should be thrown");
      } catch (NakadiException e) {
        numberOfNakadiExceptions++;
      }
    }

    assertThat(
        "We should get a NakadiException for every call",
        numberOfNakadiExceptions,
        equalTo(exceptions.size()));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void whenFetchNextOffsetsThenOk() throws NakadiException {

    // ARRANGE //
    final List<String> partitions =
        ImmutableList.of(randomUIntAsString(), randomUIntAsString(), randomUIntAsString());
    final Map<String, String> cursors =
        partitions.stream().collect(Collectors.toMap(identity(), p -> randomULongAsString()));

    final KafkaConsumer<String, String> kafkaConsumerMock = mock(KafkaConsumer.class);
    final Map<String, String> expectedOffsets = Maps.newHashMap();
    partitions.forEach(
        partition -> {
          final Long nextOffset = randomULong();
          when(kafkaConsumerMock.position(new TopicPartition(TOPIC, Integer.valueOf(partition))))
              .thenReturn(nextOffset);
          expectedOffsets.put(partition, Long.toString(nextOffset));
        });

    final KafkaFactory kafkaFactoryMock = createKafkaFactoryMock(kafkaConsumerMock);

    // ACT //
    final NakadiKafkaConsumer consumer =
        new NakadiKafkaConsumer(kafkaFactoryMock, TOPIC, cursors, POLL_TIMEOUT);
    final Map<String, String> offsets = consumer.fetchNextOffsets();

    // ASSERT //
    assertThat(
        "We should get offsets according to what we provided in kafkaConsumerMock",
        offsets,
        equalTo(expectedOffsets));
  }

  private KafkaFactory createKafkaFactoryMock(
      final KafkaConsumer<String, String> kafkaConsumerMock) {
    final KafkaFactory kafkaFactoryMock = mock(KafkaFactory.class);
    when(kafkaFactoryMock.getConsumer()).thenReturn(kafkaConsumerMock);
    return kafkaFactoryMock;
  }
}
