package org.zalando.nakadi.service;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import static java.util.Collections.nCopies;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import static java.util.Optional.empty;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import static junit.framework.TestCase.assertSame;
import static junit.framework.TestCase.fail;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.core.Is.is;
import org.junit.Assert;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import org.zalando.nakadi.config.JsonConfig;
import org.zalando.nakadi.domain.ConsumedEvent;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.exceptions.NakadiException;
import org.zalando.nakadi.repository.db.EventTypeCache;
import org.zalando.nakadi.repository.kafka.KafkaCursor;
import org.zalando.nakadi.repository.kafka.NakadiKafkaConsumer;
import static org.zalando.nakadi.service.EventStreamWriter.BATCH_SEPARATOR;
import org.zalando.nakadi.service.converter.CursorConverterImpl;
import org.zalando.nakadi.service.timeline.TimelineService;
import static org.zalando.nakadi.utils.TestUtils.createFakeTimeline;
import static org.zalando.nakadi.utils.TestUtils.randomString;
import static org.zalando.nakadi.utils.TestUtils.waitFor;
import org.zalando.nakadi.view.Cursor;
import static uk.co.datumedge.hamcrest.json.SameJSONAs.sameJSONAs;

public abstract class EventStreamTest {

    private static final String TOPIC = randomString();
    private static final String DUMMY = "DUMMY";
    private static final Meter BYTES_FLUSHED_METER = new MetricRegistry().meter("mock");

    private static final Timeline TIMELINE = createFakeTimeline(TOPIC);
    private static CursorConverter cursorConverter;
    private static EventStreamWriterProvider writerProvider;

    private final ObjectMapper mapper = new JsonConfig().jacksonObjectMapper();

    @BeforeClass
    public static void setupMocks() {
        final TimelineService timelineService = mock(TimelineService.class);
        final EventTypeCache eventTypeCache = mock(EventTypeCache.class);
        cursorConverter = new CursorConverterImpl(eventTypeCache, timelineService);
        writerProvider = mock(EventStreamWriterProvider.class);
    }

    @Before
    public void setupWriter() {
        when(writerProvider.getWriter()).thenReturn(createWriter());
    }

    protected abstract EventStreamWriter createWriter();

    @Test(timeout = 15000)
    public void whenIOExceptionThenStreamIsClosed() throws NakadiException, InterruptedException, IOException {
        final EventStreamConfig config = EventStreamConfig
                .builder()
                .withCursors(ImmutableList.of(new NakadiCursor(TIMELINE, "0", "0")))
                .withBatchLimit(1)
                .withBatchTimeout(1)
                .build();
        final OutputStream outputStreamMock = mock(OutputStream.class);
        final EventStream eventStream = new EventStream(
                emptyConsumer(), outputStreamMock, config, mock(BlacklistService.class), cursorConverter,
                BYTES_FLUSHED_METER, writerProvider);

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
                .withCursors(ImmutableList.of(new NakadiCursor(TIMELINE, "0", "0")))
                .withBatchLimit(1)
                .withBatchTimeout(1)
                .build();
        final EventStream eventStream = new EventStream(
                emptyConsumer(), mock(OutputStream.class), config, mock(BlacklistService.class), cursorConverter,
                BYTES_FLUSHED_METER, writerProvider);
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

    @Test(timeout = 5000)
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
                BYTES_FLUSHED_METER, writerProvider);
        eventStream.streamEvents(new AtomicBoolean(true));
        // if something goes wrong - the test should fail with a timeout
    }

    @Test(timeout = 3000)
    public void whenStreamLimitIsSetThenStreamIsClosed() throws NakadiException, IOException, InterruptedException {
        final EventStreamConfig config = EventStreamConfig
                .builder()
                .withCursors(ImmutableList.of(new NakadiCursor(TIMELINE, "0", "0")))
                .withBatchLimit(1)
                .withStreamLimit(1)
                .build();
        final EventStream eventStream = new EventStream(endlessDummyConsumer(), mock(OutputStream.class), config,
                mock(BlacklistService.class), cursorConverter, BYTES_FLUSHED_METER, writerProvider);
        eventStream.streamEvents(new AtomicBoolean(true));
        // if something goes wrong - the test should fail with a timeout
    }

    @Test(timeout = 5000)
    public void whenKeepAliveLimitIsSetThenStreamIsClosed() throws NakadiException, IOException, InterruptedException {
        final EventStreamConfig config = EventStreamConfig
                .builder()
                .withCursors(ImmutableList.of(new NakadiCursor(TIMELINE, "0", "0")))
                .withBatchLimit(1)
                .withBatchTimeout(1)
                .withStreamKeepAliveLimit(1)
                .build();
        final EventStream eventStream = new EventStream(
                emptyConsumer(), mock(OutputStream.class), config, mock(BlacklistService.class), cursorConverter,
                BYTES_FLUSHED_METER, writerProvider);
        eventStream.streamEvents(new AtomicBoolean(true));
        // if something goes wrong - the test should fail with a timeout
    }

    @Test(timeout = 15000)
    public void whenNoEventsToReadThenKeepAliveIsSent() throws NakadiException, IOException, InterruptedException {
        final EventStreamConfig config = EventStreamConfig
                .builder()
                .withCursors(ImmutableList.of(new NakadiCursor(TIMELINE, "0", "000000000000000000")))
                .withBatchLimit(1)
                .withBatchTimeout(1)
                .withStreamTimeout(3)
                .build();

        final ByteArrayOutputStream out = new ByteArrayOutputStream();

        final EventStream eventStream = new EventStream(
                emptyConsumer(), out, config, mock(BlacklistService.class), cursorConverter, BYTES_FLUSHED_METER,
                writerProvider);
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
                .withCursors(ImmutableList.of(new NakadiCursor(TIMELINE, "0", String.format("%18d", 0))))
                .withBatchLimit(5)
                .withBatchTimeout(1)
                .withStreamTimeout(1)
                .build();

        final ByteArrayOutputStream out = new ByteArrayOutputStream();

        final EventStream eventStream = new EventStream(
                nCountDummyConsumerForPartition(12, "0"), out, config, mock(BlacklistService.class),
                cursorConverter, BYTES_FLUSHED_METER, writerProvider);
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
                .withCursors(ImmutableList.of(new NakadiCursor(TIMELINE, "0", "0")))
                .withBatchLimit(1)
                .withStreamLimit(4)
                .build();

        final ByteArrayOutputStream out = new ByteArrayOutputStream();

        final int eventNum = 4;
        final LinkedList<ConsumedEvent> events = new LinkedList<>(IntStream
                .range(0, eventNum)
                .boxed()
                .map(index -> new ConsumedEvent(
                        "event" + index, new NakadiCursor(TIMELINE, "0", KafkaCursor.toNakadiOffset(index))))
                .collect(Collectors.toList()));

        final EventStream eventStream =
                new EventStream(predefinedConsumer(events), out, config, mock(BlacklistService.class), cursorConverter,
                        BYTES_FLUSHED_METER, writerProvider);
        eventStream.streamEvents(new AtomicBoolean(true));

        final String[] batches = out.toString().split(BATCH_SEPARATOR);

        assertThat(batches, arrayWithSize(eventNum));
        IntStream
                .range(0, eventNum)
                .boxed()
                .forEach(index -> assertThat(
                        batches[index],
                        sameJSONAs(jsonBatch(
                                "0", KafkaCursor.toNakadiOffset(index), Optional.of(nCopies(1, "event" + index))))
                ));
    }

    @Test(timeout = 10000)
    public void whenReadFromMultiplePartitionsThenGroupedInBatchesAccordingToPartition()
            throws NakadiException, IOException, InterruptedException {

        final EventStreamConfig config = EventStreamConfig
                .builder()
                .withCursors(ImmutableList.of(
                        new NakadiCursor(TIMELINE, "0", "000000000000000000"),
                        new NakadiCursor(TIMELINE, "1", "000000000000000000"),
                        new NakadiCursor(TIMELINE, "2", "000000000000000000")))
                .withBatchLimit(2)
                .withStreamLimit(6)
                .withBatchTimeout(30)
                .build();

        final ByteArrayOutputStream out = new ByteArrayOutputStream();

        final LinkedList<ConsumedEvent> events = new LinkedList<>();
        events.add(new ConsumedEvent(DUMMY, new NakadiCursor(TIMELINE, "0", "000000000000000000")));
        events.add(new ConsumedEvent(DUMMY, new NakadiCursor(TIMELINE, "1", "000000000000000000")));
        events.add(new ConsumedEvent(DUMMY, new NakadiCursor(TIMELINE, "2", "000000000000000000")));
        events.add(new ConsumedEvent(DUMMY, new NakadiCursor(TIMELINE, "0", "000000000000000000")));
        events.add(new ConsumedEvent(DUMMY, new NakadiCursor(TIMELINE, "1", "000000000000000000")));
        events.add(new ConsumedEvent(DUMMY, new NakadiCursor(TIMELINE, "2", "000000000000000000")));

        final EventStream eventStream =
                new EventStream(predefinedConsumer(events), out, config, mock(BlacklistService.class), cursorConverter,
                        BYTES_FLUSHED_METER, writerProvider);
        eventStream.streamEvents(new AtomicBoolean(true));

        final String[] batches = out.toString().split(BATCH_SEPARATOR);

        assertThat(batches, arrayWithSize(3));
        assertThat(batches[0], sameJSONAs(jsonBatch("0", "000000000000000000", Optional.of(nCopies(2, DUMMY)))));
        assertThat(batches[1], sameJSONAs(jsonBatch("1", "000000000000000000", Optional.of(nCopies(2, DUMMY)))));
        assertThat(batches[2], sameJSONAs(jsonBatch("2", "000000000000000000", Optional.of(nCopies(2, DUMMY)))));
    }

    private static NakadiKafkaConsumer emptyConsumer() throws NakadiException {
        final NakadiKafkaConsumer nakadiKafkaConsumer = mock(NakadiKafkaConsumer.class);
        when(nakadiKafkaConsumer.readEvents()).thenReturn(Collections.emptyList());
        return nakadiKafkaConsumer;
    }

    private static NakadiKafkaConsumer endlessDummyConsumerForPartition(final String partition) throws NakadiException {
        final NakadiKafkaConsumer nakadiKafkaConsumer = mock(NakadiKafkaConsumer.class);
        when(nakadiKafkaConsumer.readEvents())
                .thenReturn(Collections.singletonList(
                        new ConsumedEvent(DUMMY, new NakadiCursor(TIMELINE, partition, "0"))));
        return nakadiKafkaConsumer;
    }

    private static NakadiKafkaConsumer nCountDummyConsumerForPartition(final int eventNum, final String partition)
            throws NakadiException {
        final NakadiKafkaConsumer nakadiKafkaConsumer = mock(NakadiKafkaConsumer.class);
        final AtomicInteger eventsToCreate = new AtomicInteger(eventNum);
        when(nakadiKafkaConsumer.readEvents()).thenAnswer(invocation -> {
            if (eventsToCreate.get() > 0) {
                eventsToCreate.set(eventsToCreate.get() - 1);
                return Collections.singletonList(
                        new ConsumedEvent(DUMMY, new NakadiCursor(TIMELINE, partition, "000000000000000000")));
            } else {
                return Collections.emptyList();
            }
        });
        return nakadiKafkaConsumer;
    }

    private static NakadiKafkaConsumer predefinedConsumer(final List<ConsumedEvent> events)
            throws NakadiException {
        final NakadiKafkaConsumer nakadiKafkaConsumer = mock(NakadiKafkaConsumer.class);
        final AtomicBoolean sent = new AtomicBoolean(false);
        when(nakadiKafkaConsumer.readEvents()).thenAnswer(invocation -> {
            if (sent.get()) {
                return Collections.emptyList();
            } else {
                sent.set(true);
                return events;
            }
        });
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

    @SuppressWarnings("unchecked")
    @Test
    public void testWriteStreamEvent() {

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final Cursor cursor = new Cursor("22", "000000000000000023");
        final ArrayList<String> events = Lists.newArrayList(
            "{\"a\":\"b\"}",
            "{\"c\":\"d\"}",
            "{\"e\":\"f\"}");

        try {
            writerProvider.getWriter().writeBatch(baos, cursor, events);
            final Map<String, Object> batch =
                mapper.readValue(baos.toString(), new TypeReference<Map<String, Object>>() {});

            final Map<String, String> cursorM = (Map<String, String>) batch.get("cursor");
            assertEquals("22", cursorM.get("partition"));
            assertEquals("000000000000000023", cursorM.get("offset"));

            final List<Map<String, String>> eventsM = (List<Map<String, String>>) batch.get("events");
            assertSame(eventsM.size(), 3);

            // check the order is preserved as well as the data via get
            assertEquals("b", eventsM.get(0).get("a"));
            assertEquals("d", eventsM.get(1).get("c"));
            assertEquals("f", eventsM.get(2).get("e"));

        } catch (IOException e) {
            fail(e.getMessage());
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testWriteStreamEventEmptyBatchProducesNoEventArray() {

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final Cursor cursor = new Cursor("11", "000000000000000012");
        final ArrayList<String> events = Lists.newArrayList();

        try {
            writerProvider.getWriter().writeBatch(baos, cursor, events);
            final String json = baos.toString();

            assertEquals("{\"cursor\":{\"partition\":\"11\",\"offset\":\"000000000000000012\"}}\n", json);

            final Map<String, Object> batch =
                mapper.readValue(json, new TypeReference<Map<String, Object>>() {});

            final Map<String, String> cursorM = (Map<String, String>) batch.get("cursor");
            assertEquals("11", cursorM.get("partition"));
            assertEquals("000000000000000012", cursorM.get("offset"));

            final List<Map<String, String>> eventsM = (List<Map<String, String>>) batch.get("events");
            // expecting events not to be written as an empty array
            assertNull(eventsM);

        } catch (IOException e) {
            fail(e.getMessage());
        }
    }
}
