package de.zalando.aruha.nakadi.service;

import com.google.common.collect.ImmutableMap;
import de.zalando.aruha.nakadi.NakadiException;
import de.zalando.aruha.nakadi.domain.ConsumedEvent;
import de.zalando.aruha.nakadi.repository.kafka.NakadiKafkaConsumer;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import sun.java2d.xr.MutableInteger;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static de.zalando.aruha.nakadi.service.EventStream.BATCH_SEPARATOR;
import static de.zalando.aruha.nakadi.utils.TestUtils.randomString;
import static de.zalando.aruha.nakadi.utils.TestUtils.randomUInt;
import static java.util.Collections.nCopies;
import static java.util.Optional.empty;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static uk.co.datumedge.hamcrest.json.SameJSONAs.sameJSONAs;

public class EventStreamTest {

    private static final String TOPIC = randomString();
    private static final String DUMMY = "DUMMY";
    private static final int PARTITION = randomUInt();

    @Before
    public void setup() throws NakadiException {

    }

    @Test
    public void whenNoExitConditionsThenStreamIsNotClosed() throws NakadiException, IOException, InterruptedException {
        final EventStreamConfig config = EventStreamConfig
                .builder()
                .withTopic(TOPIC)
                .withBatchLimit(1)
                .build();
        final EventStream eventStream = new EventStream(emptyConsumer(), mock(OutputStream.class), config);

        final Thread thread = new Thread(eventStream::streamEvents);
        thread.start();
        Thread.sleep(2000);

        assertThat("The stream should be still alive as we didn't set any exit conditions", thread.isAlive(), is(true));
    }

    @Test
    public void whenStreamTimeoutIsSetThenStreamIsClosed() throws NakadiException, IOException, InterruptedException {
        final EventStreamConfig config = EventStreamConfig
                .builder()
                .withTopic(TOPIC)
                .withBatchLimit(1)
                .withStreamTimeout(1)
                .build();
        final EventStream eventStream = new EventStream(emptyConsumer(), mock(OutputStream.class), config);

        final Thread thread = new Thread(eventStream::streamEvents);
        thread.start();
        Thread.sleep(2000);

        assertThat("The stream should be closed as we set stream timeout to 1 second", thread.isAlive(), is(false));
    }

    @Test
    public void whenStreamLimitIsSetThenStreamIsClosed() throws NakadiException, IOException, InterruptedException {
        final EventStreamConfig config = EventStreamConfig
                .builder()
                .withTopic(TOPIC)
                .withCursors(ImmutableMap.of("0", "0"))
                .withBatchLimit(1)
                .withStreamLimit(1)
                .build();
        final EventStream eventStream = new EventStream(endlessDummyConsumer(), mock(OutputStream.class), config);

        final Thread thread = new Thread(eventStream::streamEvents);
        thread.start();
        Thread.sleep(1000);

        assertThat("The stream should be closed as we set stream limit to 1 event", thread.isAlive(), is(false));
    }

    @Test
    public void whenKeepAliveLimitIsSetThenStreamIsClosed() throws NakadiException, IOException, InterruptedException {
        final EventStreamConfig config = EventStreamConfig
                .builder()
                .withTopic(TOPIC)
                .withCursors(ImmutableMap.of("0", "0"))
                .withBatchLimit(1)
                .withStreamKeepAliveLimit(1)
                .build();
        final EventStream eventStream = new EventStream(emptyConsumer(), mock(OutputStream.class), config);

        final Thread thread = new Thread(eventStream::streamEvents);
        thread.start();
        Thread.sleep(2000);

        assertThat("The stream should be closed as we set keep alive limit to 1", thread.isAlive(), is(false));
    }

    @Test
    public void whenNoEventsToReadThenKeepAliveIsSent() throws NakadiException, IOException, InterruptedException {
        final EventStreamConfig config = EventStreamConfig
                .builder()
                .withTopic(TOPIC)
                .withCursors(ImmutableMap.of("0", "0"))
                .withBatchLimit(1)
                .withBatchTimeout(1)
                .withStreamTimeout(3)
                .build();

        final ByteArrayOutputStream out = new ByteArrayOutputStream();

        final EventStream eventStream = new EventStream(emptyConsumer(), out, config);
        eventStream.streamEvents();

        final String[] batches = out.toString().split(BATCH_SEPARATOR);
        Arrays
                .stream(batches)
                .forEach(batch ->
                        assertThat(batch, sameJSONAs(jsonBatch("0", "0", empty()))));
    }

    @Test
    public void whenBatchSizeIsSetThenGetEventsInBatches() throws NakadiException, IOException, InterruptedException {
        final EventStreamConfig config = EventStreamConfig
                .builder()
                .withTopic(TOPIC)
                .withCursors(ImmutableMap.of("0", "0"))
                .withBatchLimit(5)
                .withBatchTimeout(2)
                .withStreamTimeout(1)
                .build();

        final ByteArrayOutputStream out = new ByteArrayOutputStream();

        final EventStream eventStream = new EventStream(nCountDummyConsumerForPartition(12, "0"), out, config);
        eventStream.streamEvents();

        final String[] batches = out.toString().split(BATCH_SEPARATOR);

        assertThat(batches, arrayWithSize(3));
        assertThat(batches[0], sameJSONAs(jsonBatch("0", "0", Optional.of(nCopies(5, DUMMY)))));
        assertThat(batches[1], sameJSONAs(jsonBatch("0", "0", Optional.of(nCopies(5, DUMMY)))));
        assertThat(batches[2], sameJSONAs(jsonBatch("0", "0", Optional.of(nCopies(2, DUMMY)))));
    }

    private static NakadiKafkaConsumer emptyConsumer() throws NakadiException {
        final NakadiKafkaConsumer nakadiKafkaConsumer = mock(NakadiKafkaConsumer.class);
        when(nakadiKafkaConsumer.readEvent()).thenReturn(empty());
        return nakadiKafkaConsumer;
    }

    private static NakadiKafkaConsumer endlessDummyConsumerForPartition(final String partition) throws NakadiException {
        final NakadiKafkaConsumer nakadiKafkaConsumer = mock(NakadiKafkaConsumer.class);
        when(nakadiKafkaConsumer.readEvent()).thenReturn(Optional.of(new ConsumedEvent(DUMMY, TOPIC, partition, "0")));
        return nakadiKafkaConsumer;
    }

    private static NakadiKafkaConsumer nCountDummyConsumerForPartition(final int eventNum, final String partition)
            throws NakadiException {
        final NakadiKafkaConsumer nakadiKafkaConsumer = mock(NakadiKafkaConsumer.class);
        final AtomicInteger eventsToCreate = new AtomicInteger(eventNum);
        when(nakadiKafkaConsumer.readEvent()).thenAnswer(invocation -> {
            if (eventsToCreate.get() > 0) {
                eventsToCreate.set(eventsToCreate.get() - 1);
                return Optional.of(new ConsumedEvent(DUMMY, TOPIC, partition, "0"));
            }
            else {
                return empty();
            }
        });
        return nakadiKafkaConsumer;
    }

    private static NakadiKafkaConsumer endlessDummyConsumer() throws NakadiException {
        return endlessDummyConsumerForPartition("0");
    }

    private static String jsonBatch(final String partition, final String offset, final Optional<List<String>> eventsOrNone) {
        final String eventsStr = eventsOrNone
                .map(events -> {
                    final StringBuilder builder = new StringBuilder(",\"events\":[");
                    events.forEach(event -> builder.append("\"").append(event).append("\","));
                    builder.deleteCharAt(builder.length() - 1).append("]");
                    return builder.toString();
                })
                .orElse("");
        return String.format("{\"cursor\":{\"partition\":\"%s\",\"offset\":\"%s\"}%s}", partition, offset, eventsStr);
    }

}
