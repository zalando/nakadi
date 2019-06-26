package org.zalando.nakadi.service.subscription.state;

import org.junit.BeforeClass;
import org.junit.Test;
import org.zalando.nakadi.domain.ConsumedEvent;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.storage.Storage;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.repository.kafka.KafkaCursor;

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
    private static final Comparator<NakadiCursor> COMP = Comparator.comparing(NakadiCursor::getOffset);

    @BeforeClass
    public static void initTimeline() {
        when(firstTimeline.getStorage()).thenReturn(new Storage("", Storage.Type.KAFKA));
    }

    private static NakadiCursor createCursor(final long offset) {
        return new KafkaCursor("x", 0, offset).toNakadiCursor(firstTimeline);
    }

    @Test
    public void onNewOffsetsShouldSupportRollback() {
        final PartitionData pd = new PartitionData(COMP, null, createCursor(100L), System.currentTimeMillis());
        final PartitionData.CommitResult cr = pd.onCommitOffset(createCursor(90L));

        assertEquals(0L, cr.committedCount);
        assertEquals(true, cr.seekOnKafka);
        assertEquals(90L, Long.parseLong(pd.getSentOffset().getOffset()));
        assertEquals(0L, pd.getUnconfirmed());
    }

    @Test
    public void onNewOffsetsShouldSupportCommitInFuture() {
        final PartitionData pd = new PartitionData(COMP, null, createCursor(100L), System.currentTimeMillis());
        final PartitionData.CommitResult cr = pd.onCommitOffset(createCursor(110L));

        assertEquals(0L, cr.committedCount);
        assertEquals(true, cr.seekOnKafka);
        assertEquals(110L, Long.parseLong(pd.getSentOffset().getOffset()));
        assertEquals(0L, pd.getUnconfirmed());
    }

    @Test
    public void normalOperationShouldNotReconfigureKafkaConsumer() {
        final PartitionData pd = new PartitionData(COMP, null, createCursor(100L), System.currentTimeMillis());
        for (long i = 0; i < 100; ++i) {
            pd.addEvent(new ConsumedEvent(("test_" + i).getBytes(), createCursor(100L + i + 1), 0));
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
        final PartitionData pd = new PartitionData(COMP, null, createCursor(100L), System.currentTimeMillis());
        for (int i = 0; i < 100; ++i) {
            pd.takeEventsToStream(currentTimeMillis(), 10, 0L, false);
            assertEquals(i + 1, pd.getKeepAliveInARow());
        }
        pd.addEvent(new ConsumedEvent("".getBytes(), createCursor(101L), 0));
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

        final PartitionData pd = new PartitionData(COMP, null, createCursor(100L), currentTime);
        for (int i = 0; i < 100; ++i) {
            pd.addEvent(new ConsumedEvent("test".getBytes(), createCursor(i + 100L + 1), 0));
        }
        List<ConsumedEvent> data = pd.takeEventsToStream(currentTime, 1000, timeout, false);
        assertNull(data);
        assertEquals(0, pd.getKeepAliveInARow());

        currentTime += timeout + 1;

        data = pd.takeEventsToStream(currentTime, 1000, timeout, false);
        assertNotNull(data);
        assertEquals(100, data.size());

        for (int i = 100; i < 200; ++i) {
            pd.addEvent(new ConsumedEvent("test".getBytes(), createCursor(i + 100L + 1), 0));
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
        final PartitionData pd = new PartitionData(COMP, null, createCursor(100L), System.currentTimeMillis());
        for (int i = 0; i < 100; ++i) {
            pd.addEvent(new ConsumedEvent("test".getBytes(), createCursor(i + 100L + 1), 0));
        }
        assertNull(pd.takeEventsToStream(currentTimeMillis(), 1000, timeout, false));
        final List<ConsumedEvent> eventsToStream = pd.takeEventsToStream(currentTimeMillis(), 99, timeout, false);
        assertNotNull(eventsToStream);
        assertEquals(99, eventsToStream.size());
    }

    @Test
    public void eventsShouldBeStreamedOnStreamTimeout() {
        final long timeout = TimeUnit.SECONDS.toMillis(100);
        final PartitionData pd = new PartitionData(COMP, null, createCursor(100L), System.currentTimeMillis());
        for (int i = 0; i < 10; ++i) {
            pd.addEvent(new ConsumedEvent("test".getBytes(), createCursor(i), 0));
        }
        assertEquals(10, pd.takeEventsToStream(currentTimeMillis(), 100, timeout, true).size());
    }

    @Test
    public void noEmptyBatchShouldBeStreamedOnStreamTimeoutWhenNoEvents() {
        final long timeout = TimeUnit.SECONDS.toMillis(100);
        final PartitionData pd = new PartitionData(COMP, null, createCursor(100L), System.currentTimeMillis());
        for (int i = 0; i < 10; ++i) {
            pd.addEvent(new ConsumedEvent("test".getBytes(), createCursor(i), 0));
        }
        assertNull(pd.takeEventsToStream(currentTimeMillis(), 0, timeout, true));
    }
}
