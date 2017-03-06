package org.zalando.nakadi.service;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zalando.nakadi.domain.ConsumedEvent;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.exceptions.NakadiException;
import org.zalando.nakadi.repository.kafka.NakadiKafkaConsumer;
import org.zalando.nakadi.util.FeatureToggleService;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Collections.nCopies;
import static java.util.Optional.empty;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.core.Is.is;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.zalando.nakadi.service.EventStream.BATCH_SEPARATOR;
import static org.zalando.nakadi.utils.TestUtils.randomString;
import static org.zalando.nakadi.utils.TestUtils.waitFor;
import static uk.co.datumedge.hamcrest.json.SameJSONAs.sameJSONAs;

/*
 This is a full clone of EventStreamTest with SEND_BATCH_VIA_OUTPUT_STREAM enabled.

 todo: When the flag is lifted and the approach becomes the default, this can be removed.
  */
public class EventStreamTestSendBatchViaOutputStream {

    private static final String TOPIC = randomString();
    private static final String DUMMY = "DUMMY";
    private static final Meter BYTES_FLUSHED_METER = new MetricRegistry().meter("mock");

    private static CursorConverter cursorConverter;
    private static FeatureToggleService featureToggleService;

    @BeforeClass
    public static void createCursorConverter() {
        featureToggleService = mock(FeatureToggleService.class);
        when(featureToggleService.isFeatureEnabled(eq(FeatureToggleService.Feature.ZERO_PADDED_OFFSETS)))
                .thenReturn(true);
        // turn on direct delivery to outputstream
        when(featureToggleService.isFeatureEnabled(eq(FeatureToggleService.Feature.SEND_BATCH_VIA_OUTPUT_STREAM)))
                 .thenReturn(true);
        cursorConverter = new CursorConverter(featureToggleService);
    }

    private static NakadiKafkaConsumer emptyConsumer() throws NakadiException {
        final NakadiKafkaConsumer nakadiKafkaConsumer = mock(NakadiKafkaConsumer.class);
        when(nakadiKafkaConsumer.readEvent()).thenReturn(empty());
        return nakadiKafkaConsumer;
    }

    private static NakadiKafkaConsumer endlessDummyConsumerForPartition(final String partition) throws NakadiException {
        final NakadiKafkaConsumer nakadiKafkaConsumer = mock(NakadiKafkaConsumer.class);
        when(nakadiKafkaConsumer.readEvent())
                .thenReturn(Optional.of(new ConsumedEvent(DUMMY, new NakadiCursor(TOPIC, partition, "0"))));
        return nakadiKafkaConsumer;
    }

    private static NakadiKafkaConsumer nCountDummyConsumerForPartition(final int eventNum, final String partition)
            throws NakadiException {
        final NakadiKafkaConsumer nakadiKafkaConsumer = mock(NakadiKafkaConsumer.class);
        final AtomicInteger eventsToCreate = new AtomicInteger(eventNum);
        when(nakadiKafkaConsumer.readEvent()).thenAnswer(invocation -> {
            if (eventsToCreate.get() > 0) {
                eventsToCreate.set(eventsToCreate.get() - 1);
                return Optional.of(new ConsumedEvent(DUMMY, new NakadiCursor(TOPIC, partition, "0")));
            } else {
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

    private static String jsonBatch(final String partition, final String offset,
                                    final Optional<List<String>> eventsOrNone) {
        return jsonBatch(partition, offset, eventsOrNone, Optional.empty());
    }

    private static String jsonBatch(final String partition, final String offset,
                                    final Optional<List<String>> eventsOrNone, final Optional<String> metadata) {
        final String eventsStr = eventsOrNone
                .map(events -> {
                    final StringBuilder builder = new StringBuilder(",\"events\":[");
                    events.forEach(event -> builder.append("\"").append(event).append("\","));
                    builder.deleteCharAt(builder.length() - 1).append("]");
                    return builder.toString();
                })
                .orElse("");
        final String metadataStr = metadata.map(m -> ",\"metadata\":{\"debug\":\"" + m + "\"}").orElse("");

        return String.format("{\"cursor\":{\"partition\":\"%s\",\"offset\":\"%s\"}%s%s}", partition, offset, eventsStr,
                metadataStr);
    }

    @Test(timeout = 15000)
    public void whenIOExceptionThenStreamIsClosed() throws NakadiException, InterruptedException, IOException {
        final EventStreamConfig config = EventStreamConfig
                .builder()
                .withCursors(ImmutableList.of(new NakadiCursor(TOPIC, "0", "0")))
                .withBatchLimit(1)
                .withBatchTimeout(1)
                .build();
        final OutputStream outputStreamMock = mock(OutputStream.class);
        final EventStream eventStream = new EventStream(
                emptyConsumer(), outputStreamMock, config, mock(BlacklistService.class), cursorConverter,
                BYTES_FLUSHED_METER, featureToggleService);

        final Thread thread = new Thread(() -> eventStream.streamEvents(new AtomicBoolean(true)));
        thread.start();

        Thread.sleep(3000);
        assertThat("As there are no exit conditions in config - the thread should be running",
                thread.isAlive(), is(true));

        // simulation of client closing the connection: this will end the eventStream
        doThrow(new IOException()).when(outputStreamMock).flush();

        Thread.sleep(5000);
        assertThat("The thread should be dead now, as we simulated that client closed connection",
                thread.isAlive(), is(false));
        thread.join();
    }

    @Test(timeout = 10000)
    public void whenCrutchWorkedThenStreamIsClosed() throws NakadiException, InterruptedException, IOException {
        final EventStreamConfig config = EventStreamConfig
                .builder()
                .withCursors(ImmutableList.of(new NakadiCursor(TOPIC, "0", "0")))
                .withBatchLimit(1)
                .withBatchTimeout(1)
                .build();
        final EventStream eventStream = new EventStream(
                emptyConsumer(), mock(OutputStream.class), config, mock(BlacklistService.class), cursorConverter,
                BYTES_FLUSHED_METER, featureToggleService);
        final AtomicBoolean streamOpen = new AtomicBoolean(true);
        final Thread thread = new Thread(() -> eventStream.streamEvents(streamOpen));
        thread.start();

        Thread.sleep(TimeUnit.SECONDS.toMillis(1));
        waitFor(() -> Assert.assertTrue(thread.isAlive()));

        // simulation of client closing the connection using crutch
        streamOpen.set(false);

        waitFor(() -> Assert.assertFalse(thread.isAlive()), TimeUnit.SECONDS.toMillis(3));
        assertThat("The thread should be dead now, as we simulated that client closed connection",
                thread.isAlive(), is(false));
        thread.join();
    }

    @Test(timeout = 3000)
    public void whenStreamTimeoutIsSetThenStreamIsClosed() throws NakadiException, IOException, InterruptedException {
        final EventStreamConfig config = EventStreamConfig
                .builder()
                .withBatchLimit(1)
                .withStreamTimeout(1)
                .withBatchTimeout(1)
                .withCursors(new ArrayList<>())
                .build();
        final EventStream eventStream = new EventStream(
                emptyConsumer(), mock(OutputStream.class), config, mock(BlacklistService.class), cursorConverter,
                BYTES_FLUSHED_METER, featureToggleService);
        eventStream.streamEvents(new AtomicBoolean(true));
        // if something goes wrong - the test should fail with a timeout
    }

    @Test(timeout = 3000)
    public void whenStreamLimitIsSetThenStreamIsClosed() throws NakadiException, IOException, InterruptedException {
        final EventStreamConfig config = EventStreamConfig
                .builder()
                .withCursors(ImmutableList.of(new NakadiCursor(TOPIC, "0", "0")))
                .withBatchLimit(1)
                .withStreamLimit(1)
                .build();
        final EventStream eventStream = new EventStream(endlessDummyConsumer(), mock(OutputStream.class), config,
                mock(BlacklistService.class), cursorConverter, BYTES_FLUSHED_METER, featureToggleService);
        eventStream.streamEvents(new AtomicBoolean(true));
        // if something goes wrong - the test should fail with a timeout
    }

    @Test(timeout = 5000)
    public void whenKeepAliveLimitIsSetThenStreamIsClosed() throws NakadiException, IOException, InterruptedException {
        final EventStreamConfig config = EventStreamConfig
                .builder()
                .withCursors(ImmutableList.of(new NakadiCursor(TOPIC, "0", "0")))
                .withBatchLimit(1)
                .withBatchTimeout(1)
                .withStreamKeepAliveLimit(1)
                .build();
        final EventStream eventStream = new EventStream(
                emptyConsumer(), mock(OutputStream.class), config, mock(BlacklistService.class), cursorConverter,
                BYTES_FLUSHED_METER, featureToggleService);
        eventStream.streamEvents(new AtomicBoolean(true));
        // if something goes wrong - the test should fail with a timeout
    }

    @Test(timeout = 15000)
    public void whenNoEventsToReadThenKeepAliveIsSent() throws NakadiException, IOException, InterruptedException {
        final EventStreamConfig config = EventStreamConfig
                .builder()
                .withCursors(ImmutableList.of(new NakadiCursor(TOPIC, "0", "0")))
                .withBatchLimit(1)
                .withBatchTimeout(1)
                .withStreamTimeout(3)
                .build();

        final ByteArrayOutputStream out = new ByteArrayOutputStream();

        final EventStream eventStream = new EventStream(
                emptyConsumer(), out, config, mock(BlacklistService.class), cursorConverter, BYTES_FLUSHED_METER, featureToggleService);
        eventStream.streamEvents(new AtomicBoolean(true));

        final String[] batches = out.toString().split(BATCH_SEPARATOR);

        Arrays
                .stream(batches)
                .forEach(batch ->
                        assertThat(batch, sameJSONAs(jsonBatch("0", "000000000000000000", empty()))));
    }

    @Test(timeout = 10000)
    public void whenBatchSizeIsSetThenGetEventsInBatches() throws NakadiException, IOException, InterruptedException {
        final EventStreamConfig config = EventStreamConfig
                .builder()
                .withCursors(ImmutableList.of(new NakadiCursor(TOPIC, "0", "0")))
                .withBatchLimit(5)
                .withBatchTimeout(1)
                .withStreamTimeout(1)
                .build();

        final ByteArrayOutputStream out = new ByteArrayOutputStream();

        final EventStream eventStream = new EventStream(
                nCountDummyConsumerForPartition(12, "0"), out, config, mock(BlacklistService.class), cursorConverter,
                BYTES_FLUSHED_METER, featureToggleService);
        eventStream.streamEvents(new AtomicBoolean(true));

        final String[] batches = out.toString().split(BATCH_SEPARATOR);

        assertThat(batches, arrayWithSize(3));
        assertThat(batches[0], sameJSONAs(jsonBatch("0", "000000000000000000", Optional.of(nCopies(5, DUMMY)))));
        assertThat(batches[1], sameJSONAs(jsonBatch("0", "000000000000000000", Optional.of(nCopies(5, DUMMY)))));
        assertThat(batches[2], sameJSONAs(jsonBatch("0", "000000000000000000", Optional.of(nCopies(2, DUMMY)))));
    }

    @Test(timeout = 10000)
    public void whenReadingEventsTheOrderIsCorrect() throws NakadiException, IOException, InterruptedException {
        final EventStreamConfig config = EventStreamConfig
                .builder()
                .withCursors(ImmutableList.of(new NakadiCursor(TOPIC, "0", "0")))
                .withBatchLimit(1)
                .withStreamLimit(4)
                .build();

        final ByteArrayOutputStream out = new ByteArrayOutputStream();

        final int eventNum = 4;
        final LinkedList<ConsumedEvent> events = new LinkedList<>(IntStream
                .range(0, eventNum)
                .boxed()
                .map(index ->
                        new ConsumedEvent("event" + index, new NakadiCursor(TOPIC, "0", String.valueOf(index))))
                .collect(Collectors.toList()));

        final EventStream eventStream =
                new EventStream(predefinedConsumer(events), out, config, mock(BlacklistService.class), cursorConverter,
                        BYTES_FLUSHED_METER, featureToggleService);
        eventStream.streamEvents(new AtomicBoolean(true));

        final String[] batches = out.toString().split(BATCH_SEPARATOR);

        assertThat(batches, arrayWithSize(eventNum));
        IntStream
                .range(0, eventNum)
                .boxed()
                .forEach(index -> assertThat(
                        batches[index],
                        sameJSONAs(jsonBatch(
                                "0", String.format("%018d", index), Optional.of(nCopies(1, "event" + index))))
                ));
    }

    @Test(timeout = 10000)
    public void whenReadFromMultiplePartitionsThenGroupedInBatchesAccordingToPartition()
            throws NakadiException, IOException, InterruptedException {

        final EventStreamConfig config = EventStreamConfig
                .builder()
                .withCursors(ImmutableList.of(
                        new NakadiCursor(TOPIC, "0", "0"),
                        new NakadiCursor(TOPIC, "1", "0"),
                        new NakadiCursor(TOPIC, "2", "0")))
                .withBatchLimit(2)
                .withStreamLimit(6)
                .withBatchTimeout(30)
                .build();

        final ByteArrayOutputStream out = new ByteArrayOutputStream();

        final LinkedList<ConsumedEvent> events = new LinkedList<>();
        events.add(new ConsumedEvent(DUMMY, new NakadiCursor(TOPIC, "0", "0")));
        events.add(new ConsumedEvent(DUMMY, new NakadiCursor(TOPIC, "1", "0")));
        events.add(new ConsumedEvent(DUMMY, new NakadiCursor(TOPIC, "2", "0")));
        events.add(new ConsumedEvent(DUMMY, new NakadiCursor(TOPIC, "0", "0")));
        events.add(new ConsumedEvent(DUMMY, new NakadiCursor(TOPIC, "1", "0")));
        events.add(new ConsumedEvent(DUMMY, new NakadiCursor(TOPIC, "2", "0")));

        final EventStream eventStream =
                new EventStream(predefinedConsumer(events), out, config, mock(BlacklistService.class), cursorConverter,
                        BYTES_FLUSHED_METER, featureToggleService);
        eventStream.streamEvents(new AtomicBoolean(true));

        final String[] batches = out.toString().split(BATCH_SEPARATOR);

        assertThat(batches, arrayWithSize(3));
        assertThat(batches[0], sameJSONAs(jsonBatch("0", "000000000000000000", Optional.of(nCopies(2, DUMMY)))));
        assertThat(batches[1], sameJSONAs(jsonBatch("1", "000000000000000000", Optional.of(nCopies(2, DUMMY)))));
        assertThat(batches[2], sameJSONAs(jsonBatch("2", "000000000000000000", Optional.of(nCopies(2, DUMMY)))));
    }

}
