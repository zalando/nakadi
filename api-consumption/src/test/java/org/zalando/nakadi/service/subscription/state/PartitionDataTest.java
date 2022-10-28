package org.zalando.nakadi.service.subscription.state;

import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.zalando.nakadi.domain.ConsumedEvent;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.domain.storage.Storage;
import org.zalando.nakadi.repository.kafka.KafkaCursor;
import org.zalando.nakadi.service.CursorOperationsService;
import org.zalando.nakadi.service.timeline.TimelineService;

import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.lang.System.currentTimeMillis;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PartitionDataTest {

    private static Timeline firstTimeline = mock(Timeline.class);
    private static TimelineService timelineService = Mockito.mock(TimelineService.class);
    private static final Comparator<NakadiCursor> COMP = Comparator.comparing(NakadiCursor::getOffset);

    @BeforeClass
    public static void initTimeline() {
        when(firstTimeline.getStorage()).thenReturn(new Storage("", Storage.Type.KAFKA));
        when(firstTimeline.getOrder()).thenReturn(0);
    }

    private static NakadiCursor createCursor(final long offset) {
        return new KafkaCursor("x", 0, offset).toNakadiCursor(firstTimeline);
    }

    @Test
    public void onNewOffsetsShouldSupportRollback() {
        final PartitionData pd = new PartitionData(COMP, null, createCursor(100L), System.currentTimeMillis(), 0L,
                new CursorOperationsService(timelineService));
        final PartitionData.CommitResult cr = pd.onCommitOffset(createCursor(90L));

        assertEquals(0L, cr.committedCount);
        assertEquals(true, cr.seekOnKafka);
        assertEquals(90L, Long.parseLong(pd.getSentOffset().getOffset()));
        assertEquals(0L, pd.getUnconfirmed());
    }

    @Test
    public void onNewOffsetsShouldSupportCommitInFuture() {
        final PartitionData pd = new PartitionData(COMP, null, createCursor(100L), System.currentTimeMillis(), 0L,
                new CursorOperationsService(timelineService));
        final PartitionData.CommitResult cr = pd.onCommitOffset(createCursor(110L));

        assertEquals(10L, cr.committedCount);
        assertEquals(true, cr.seekOnKafka);
        assertEquals(110L, Long.parseLong(pd.getSentOffset().getOffset()));
        assertEquals(0L, pd.getUnconfirmed());
    }

    @Test
    public void normalOperationShouldNotReconfigureKafkaConsumer() {
        final PartitionData pd = new PartitionData(COMP, null, createCursor(100L), System.currentTimeMillis(), 0L,
                new CursorOperationsService(timelineService));
        for (long i = 0; i < 100; ++i) {
            pd.addEvent(new ConsumedEvent(("test_" + i).getBytes(), createCursor(100L + i + 1), 0, null));
        }
        // Now say to it that it was sent
        pd.takeEventsToStream(currentTimeMillis(), 1000, 0L, false);
        assertEquals(100L, pd.getUnconfirmed());
        for (long i = 0; i < 10; ++i) {
            final PartitionData.CommitResult cr = pd.onCommitOffset(createCursor(110L + i * 10L));
            assertEquals(10L, cr.committedCount);
            assertFalse(cr.seekOnKafka);
            assertEquals(90L - i * 10L, pd.getUnconfirmed());
        }
    }

    @Test
    public void keepAliveCountShouldIncreaseOnEachEmptyCall() {
        final PartitionData pd = new PartitionData(COMP, null, createCursor(100L), System.currentTimeMillis(), 0L,
                new CursorOperationsService(timelineService));
        for (int i = 0; i < 100; ++i) {
            pd.takeEventsToStream(currentTimeMillis(), 10, 0L, false);
            assertEquals(i + 1, pd.getKeepAliveInARow());
        }
        pd.addEvent(new ConsumedEvent("".getBytes(), createCursor(101L), 0, null));
        assertEquals(100, pd.getKeepAliveInARow());
        pd.takeEventsToStream(currentTimeMillis(), 10, 0L, false);
        assertEquals(0, pd.getKeepAliveInARow());
        pd.takeEventsToStream(currentTimeMillis(), 10, 0L, false);
        assertEquals(1, pd.getKeepAliveInARow());
    }

    @Test
    public void eventsShouldBeStreamedOnTimeout() {
        final long timeout = TimeUnit.SECONDS.toMillis(1);
        long currentTime = System.currentTimeMillis();

        final PartitionData pd = new PartitionData(COMP, null, createCursor(100L), currentTime, 0L,
                new CursorOperationsService(timelineService));
        for (int i = 0; i < 100; ++i) {
            pd.addEvent(new ConsumedEvent("test".getBytes(), createCursor(i + 100L + 1), 0, null));
        }
        List<ConsumedEvent> data = pd.takeEventsToStream(currentTime, 1000, timeout, false);
        assertNull(data);
        assertEquals(0, pd.getKeepAliveInARow());

        currentTime += timeout + 1;

        data = pd.takeEventsToStream(currentTime, 1000, timeout, false);
        assertNotNull(data);
        assertEquals(100, data.size());

        for (int i = 100; i < 200; ++i) {
            pd.addEvent(new ConsumedEvent("test".getBytes(), createCursor(i + 100L + 1), 0, null));
        }
        data = pd.takeEventsToStream(currentTime, 1000, timeout, false);
        assertNull(data);
        assertEquals(0, pd.getKeepAliveInARow());

        currentTime += timeout + 1;

        data = pd.takeEventsToStream(currentTime, 1000, timeout, false);
        assertNotNull(data);
        assertEquals(100, data.size());
    }

    @Test
    public void eventsShouldBeStreamedOnBatchSize() {
        final long timeout = TimeUnit.SECONDS.toMillis(1);
        final PartitionData pd = new PartitionData(COMP, null, createCursor(100L), System.currentTimeMillis(), 0L,
                new CursorOperationsService(timelineService));
        for (int i = 0; i < 100; ++i) {
            pd.addEvent(new ConsumedEvent("test".getBytes(), createCursor(i + 100L + 1), 0, null));
        }
        assertNull(pd.takeEventsToStream(currentTimeMillis(), 1000, timeout, false));
        final List<ConsumedEvent> eventsToStream = pd.takeEventsToStream(currentTimeMillis(), 99, timeout, false);
        assertNotNull(eventsToStream);
        assertEquals(99, eventsToStream.size());
    }

    @Test
    public void streamsBatchesWithSingleEventForBatchTimespan() {
        final long currentTime = System.currentTimeMillis();
        final PartitionData pd = new PartitionData(COMP, null, createCursor(100L), currentTime, 5,
                new CursorOperationsService(timelineService));

        pd.addEvent(new ConsumedEvent("test".getBytes(), createCursor(0), currentTime + 1, null));

        assertEquals(null, pd.takeEventsToStream(currentTime, 100, 100, false)); // initialize window to [1, 6)

        pd.addEvent(new ConsumedEvent("test".getBytes(), createCursor(1), currentTime + 12, null));

        assertEquals(1, pd.takeEventsToStream(currentTime, 100, 100, false).size()); // [1, 6)
        assertEquals(1, pd.takeEventsToStream(currentTime, 100, 100, false).size()); // [6, 11)
    }

    @Test
    public void eventsShouldBeStreamedOnTimespanReached() {
        final PartitionData pd = new PartitionData(COMP, null, createCursor(100L), System.currentTimeMillis(), 5,
                new CursorOperationsService(timelineService));
        final long timeout = TimeUnit.SECONDS.toMillis(100);

        final int[] timestamps = new int[]{2, 3, 4, 5, 6, 7, 9, 10, 11, 12, 14, 17, 19, 20, 30};

        for (int i = 0; i < timestamps.length; ++i) {
            pd.addEvent(new ConsumedEvent("test".getBytes(), createCursor(i), timestamps[i], null));
        }

        final int[] sizes = new int[]{
                5, // [2, 7) = 2, 3, 4, 5, 6
                4, // [7, 12) = 7, 9, 10, 11
                2, // [12, 17) = 12, 14
                3, // [17, 22) = 17, 19, 20
                1 // [22, 27) = 30,
        };

        for (int i = 0; i < sizes.length; i++) {
            assertEquals(sizes[i], pd.takeEventsToStream(currentTimeMillis(), 100, timeout, true).size());
        }

        pd.addEvent(new ConsumedEvent("test".getBytes(), createCursor(timestamps.length), 35, null));
        assertEquals(1, pd.takeEventsToStream(currentTimeMillis(), 100, timeout, true).size());
    }

    @Test
    public void testBatchTimespanIsTriggeredByLastEventsTimestamp() {
        final PartitionData pd = new PartitionData(COMP, null, createCursor(100L), System.currentTimeMillis(), 5,
                new CursorOperationsService(timelineService));
        final long timeout = TimeUnit.SECONDS.toMillis(100);

        // under some circumstances, Kafka might contain events with out of order timestamps. But since the
        // batch_timespan parameter is based on seconds, small inconsistencies in timestamp should not be a problem.
        final int[] timestamps = new int[]{2, 3, 4, 5, 2, 4, 4, 2, 9, 1};

        for (int i = 0; i < timestamps.length; ++i) {
            pd.addEvent(new ConsumedEvent("test".getBytes(), createCursor(i), timestamps[i], null));
        }

        // Nothing is streamed, even though there is one event whose timestamp is 9 (higher than the minimum of 7
        // required to stream 5 milliseconds of data, given the first event has timestamp 2)
        assertEquals(null, pd.takeEventsToStream(currentTimeMillis(), 100, timeout, false));

        // Even though 7 triggers the flushing, it only streams until it finds 9
        pd.addEvent(new ConsumedEvent("test".getBytes(), createCursor(timestamps.length), 8, null));
        assertEquals(8, pd.takeEventsToStream(currentTimeMillis(), 100, timeout, false)
                .size()); // [2, 7)

    }

    @Test
    public void eventsShouldBeStreamedOnStreamTimeout() {
        final long timeout = TimeUnit.SECONDS.toMillis(100);
        final PartitionData pd = new PartitionData(COMP, null, createCursor(100L), System.currentTimeMillis(), 0L,
                new CursorOperationsService(timelineService));
        for (int i = 0; i < 10; ++i) {
            pd.addEvent(new ConsumedEvent("test".getBytes(), createCursor(i), 0, null));
        }
        assertEquals(10, pd.takeEventsToStream(currentTimeMillis(), 100, timeout, true).size());
    }

    @Test
    public void noEmptyBatchShouldBeStreamedOnStreamTimeoutWhenNoEvents() {
        final long timeout = TimeUnit.SECONDS.toMillis(100);
        final PartitionData pd = new PartitionData(COMP, null, createCursor(100L), System.currentTimeMillis(), 0L,
                new CursorOperationsService(timelineService));
        for (int i = 0; i < 10; ++i) {
            pd.addEvent(new ConsumedEvent("test".getBytes(), createCursor(i), 0, null));
        }
        assertNull(pd.takeEventsToStream(currentTimeMillis(), 0, timeout, true));
    }
}
