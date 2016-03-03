package de.zalando.aruha.nakadi.service;

import com.google.common.collect.ImmutableMap;
import de.zalando.aruha.nakadi.exceptions.NakadiException;
import de.zalando.aruha.nakadi.domain.ConsumedEvent;
import de.zalando.aruha.nakadi.repository.kafka.NakadiKafkaConsumer;
import org.junit.Ignore;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static de.zalando.aruha.nakadi.service.EventStream.BATCH_SEPARATOR;
import static de.zalando.aruha.nakadi.utils.TestUtils.randomString;
import static java.util.Collections.nCopies;
import static java.util.Optional.empty;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static uk.co.datumedge.hamcrest.json.SameJSONAs.sameJSONAs;

public class EventStreamTest {

    private static final String TOPIC = randomString();
    private static final String DUMMY = "DUMMY";

    @Test(timeout = 10000)
    @Ignore("This test blinks quite often; disabled for now")
    public void whenNoExitConditionsThenStreamIsNotClosed() throws NakadiException, InterruptedException, IOException {
        final EventStreamConfig config = EventStreamConfig
                .builder()
                .withCursors(ImmutableMap.of("0", "0"))
                .withTopic(TOPIC)
                .withBatchLimit(1)
                .withBatchTimeout(1)
                .build();
        final OutputStream outputStreamMock = mock(OutputStream.class);
        final EventStream eventStream = new EventStream(emptyConsumer(), outputStreamMock, config);

        final Thread thread = new Thread(eventStream::streamEvents);
        thread.start();

        Thread.sleep(3000);
        assertThat("As there are no exit conditions in config - the thread should be running",
                thread.isAlive(), is(true));

        // simulation of client closing the connection: this will end the eventStream
        doThrow(new IOException()).when(outputStreamMock).flush();

        Thread.sleep(3000);
        assertThat("The thread should be dead now, as we simulated that client closed connection",
                thread.isAlive(), is(false));
        thread.join();
    }

    @Test(timeout = 3000)
    public void whenStreamTimeoutIsSetThenStreamIsClosed() throws NakadiException, IOException, InterruptedException {
        final EventStreamConfig config = EventStreamConfig
                .builder()
                .withTopic(TOPIC)
                .withBatchLimit(1)
                .withStreamTimeout(1)
                .build();
        final EventStream eventStream = new EventStream(emptyConsumer(), mock(OutputStream.class), config);
        eventStream.streamEvents();
        // if something goes wrong - the test should fail with a timeout
    }

    @Test(timeout = 3000)
    public void whenStreamLimitIsSetThenStreamIsClosed() throws NakadiException, IOException, InterruptedException {
        final EventStreamConfig config = EventStreamConfig
                .builder()
                .withTopic(TOPIC)
                .withCursors(ImmutableMap.of("0", "0"))
                .withBatchLimit(1)
                .withStreamLimit(1)
                .build();
        final EventStream eventStream = new EventStream(endlessDummyConsumer(), mock(OutputStream.class), config);
        eventStream.streamEvents();
        // if something goes wrong - the test should fail with a timeout
    }

    @Test(timeout = 3000)
    public void whenKeepAliveLimitIsSetThenStreamIsClosed() throws NakadiException, IOException, InterruptedException {
        final EventStreamConfig config = EventStreamConfig
                .builder()
                .withTopic(TOPIC)
                .withCursors(ImmutableMap.of("0", "0"))
                .withBatchLimit(1)
                .withStreamKeepAliveLimit(1)
                .build();
        final EventStream eventStream = new EventStream(emptyConsumer(), mock(OutputStream.class), config);
        eventStream.streamEvents();
        // if something goes wrong - the test should fail with a timeout
    }

    @Test(timeout = 10000)
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

    @Test(timeout = 10000)
    public void whenBatchSizeIsSetThenGetEventsInBatches() throws NakadiException, IOException, InterruptedException {
        final EventStreamConfig config = EventStreamConfig
                .builder()
                .withTopic(TOPIC)
                .withCursors(ImmutableMap.of("0", "0"))
                .withBatchLimit(5)
                .withBatchTimeout(30)
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

    @Test(timeout = 10000)
    public void whenReadingEventsTheOrderIsCorrect() throws NakadiException, IOException, InterruptedException {
        final EventStreamConfig config = EventStreamConfig
                .builder()
                .withTopic(TOPIC)
                .withCursors(ImmutableMap.of("0", "0"))
                .withBatchLimit(1)
                .withBatchTimeout(30)
                .withStreamTimeout(1)
                .build();

        final ByteArrayOutputStream out = new ByteArrayOutputStream();

        int eventNum = 4;
        final LinkedList<ConsumedEvent> events = new LinkedList<>(IntStream
                .range(0, eventNum)
                .boxed()
                .map(index ->
                        new ConsumedEvent("event" + index, TOPIC, "0", String.valueOf(index)))
                .collect(Collectors.toList()));

        final EventStream eventStream = new EventStream(predefinedConsumer(events), out, config);
        eventStream.streamEvents();

        final String[] batches = out.toString().split(BATCH_SEPARATOR);

        assertThat(batches, arrayWithSize(eventNum));
        IntStream
                .range(0, eventNum)
                .boxed()
                .forEach(index -> assertThat(
                        batches[index],
                        sameJSONAs(jsonBatch("0", String.valueOf(index), Optional.of(nCopies(1, "event" + index))))
                ));
    }

    @Test(timeout = 5000)
    public void whenReadFromMultiplePartitionsThenGroupedInBatchesAccordingToPartition()
            throws NakadiException, IOException, InterruptedException {

        final EventStreamConfig config = EventStreamConfig
                .builder()
                .withTopic(TOPIC)
                .withCursors(ImmutableMap.of(
                        "0", "0",
                        "1", "0",
                        "2", "0"))
                .withBatchLimit(2)
                .withBatchTimeout(30)
                .withStreamTimeout(1)
                .build();

        final ByteArrayOutputStream out = new ByteArrayOutputStream();

        final LinkedList<ConsumedEvent> events = new LinkedList<>();
        events.add(new ConsumedEvent(DUMMY, TOPIC, "0", "0"));
        events.add(new ConsumedEvent(DUMMY, TOPIC, "1", "0"));
        events.add(new ConsumedEvent(DUMMY, TOPIC, "2", "0"));
        events.add(new ConsumedEvent(DUMMY, TOPIC, "0", "0"));
        events.add(new ConsumedEvent(DUMMY, TOPIC, "1", "0"));
        events.add(new ConsumedEvent(DUMMY, TOPIC, "2", "0"));

        final EventStream eventStream = new EventStream(predefinedConsumer(events), out, config);
        eventStream.streamEvents();

        final String[] batches = out.toString().split(BATCH_SEPARATOR);

        assertThat(batches, arrayWithSize(3));
        assertThat(batches[0], sameJSONAs(jsonBatch("0", "0", Optional.of(nCopies(2, DUMMY)))));
        assertThat(batches[1], sameJSONAs(jsonBatch("1", "0", Optional.of(nCopies(2, DUMMY)))));
        assertThat(batches[2], sameJSONAs(jsonBatch("2", "0", Optional.of(nCopies(2, DUMMY)))));
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

    private static NakadiKafkaConsumer predefinedConsumer(final Queue<ConsumedEvent> events)
            throws NakadiException {
        final NakadiKafkaConsumer nakadiKafkaConsumer = mock(NakadiKafkaConsumer.class);
        when(nakadiKafkaConsumer.readEvent()).thenAnswer(invocation -> Optional.ofNullable(events.poll()));
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
