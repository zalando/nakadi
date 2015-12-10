package de.zalando.aruha.nakadi.repository.kafka;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import de.zalando.aruha.nakadi.NakadiException;
import de.zalando.aruha.nakadi.domain.ConsumedEvent;
import de.zalando.aruha.nakadi.utils.TestUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.Optional;
import java.util.Random;
import java.util.UUID;

import static de.zalando.aruha.nakadi.utils.TestUtils.randomString;
import static de.zalando.aruha.nakadi.utils.TestUtils.randomUInt;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class NakadiKafkaConsumerTest {

    private static final String TOPIC = "my-topic";
    private static final int PARTITION = 12;
    private static final long POLL_TIMEOUT = 100;

    @Test
    @SuppressWarnings("unchecked")
    public void whenReadEventThenOk() throws NakadiException {

        // ARRANGE //
        final String event1 = randomString();
        final String event2 = randomString();
        final int event1Offset = randomUInt();
        final int event2Offset = randomUInt();
        final ConsumerRecords<String, String> consumerRecords = new ConsumerRecords<>(ImmutableMap.of(
                new TopicPartition(TOPIC, PARTITION), ImmutableList.of(
                        new ConsumerRecord<>(TOPIC, PARTITION, event1Offset, "k1", event1),
                        new ConsumerRecord<>(TOPIC, PARTITION, event2Offset, "k2", event2)
                )
        ));
        final ConsumerRecords<String, String> emptyRecords = new ConsumerRecords<>(ImmutableMap.of());

        final KafkaConsumer<String, String> kafkaConsumerMock = mock(KafkaConsumer.class);
        final ArgumentCaptor<Long> pollTimeoutCaptor = ArgumentCaptor.forClass(Long.class);
        when(kafkaConsumerMock.poll(pollTimeoutCaptor.capture())).thenReturn(consumerRecords, emptyRecords);

        final KafkaFactory kafkaFactoryMock = mock(KafkaFactory.class);
        when(kafkaFactoryMock.createConsumer()).thenReturn(kafkaConsumerMock);
        // we mock KafkaConsumer anyway, so the cursors we pass are not really important
        final ImmutableMap<String, String> cursors = ImmutableMap.of(Integer.toString(PARTITION), "0");

        // ACT //
        final NakadiKafkaConsumer consumer = new NakadiKafkaConsumer(kafkaFactoryMock, TOPIC, cursors, POLL_TIMEOUT);
        final Optional<ConsumedEvent> consumedEvent1 = consumer.readEvent();
        final Optional<ConsumedEvent> consumedEvent2 = consumer.readEvent();
        final Optional<ConsumedEvent> consumedEvent3 = consumer.readEvent();

        // ASSERT //
        assertThat("The event we read first should not be empty", consumedEvent1.isPresent(), equalTo(true));
        assertThat("The event we read first should have the same data as first mocked ConsumerRecord",
                consumedEvent1.get(),
                equalTo(new ConsumedEvent(event1, TOPIC, Integer.toString(PARTITION), Integer.toString(event1Offset + 1))));

        assertThat("The event we read second should not be empty", consumedEvent2.isPresent(), equalTo(true));
        assertThat("The event we read second should have the same data as second mocked ConsumerRecord",
                consumedEvent2.get(),
                equalTo(new ConsumedEvent(event2, TOPIC, Integer.toString(PARTITION), Integer.toString(event2Offset + 1))));

        assertThat("The event we read third should be empty", consumedEvent3.isPresent(), equalTo(false));

        assertThat("The kafka poll should be called with timeout we defined",
                pollTimeoutCaptor.getValue(),
                equalTo(POLL_TIMEOUT));
    }

}
