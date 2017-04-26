package org.zalando.nakadi.service.subscription.state;

import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import org.zalando.nakadi.domain.ConsumedEvent;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.repository.kafka.KafkaCursor;
import static java.lang.System.currentTimeMillis;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;

public class PartitionDataTest {

    private static Timeline fakeTimeline = mock(Timeline.class);

    private static NakadiCursor createCursor(final long offset) {
        return new KafkaCursor("x", 0, offset).toNakadiCursor(fakeTimeline);
    }

    @Test
    public void onNewOffsetsShouldSupportRollback() {
        final PartitionData pd = new PartitionData(null, createCursor(100L));
        final PartitionData.CommitResult cr = pd.onCommitOffset(createCursor(90L));

        assertEquals(0L, cr.committedCount);
        assertEquals(true, cr.seekOnKafka);
        assertEquals(90L, Long.parseLong(pd.getSentOffset().getOffset()));
        assertEquals(0L, pd.getUnconfirmed());
    }

    @Test
    public void onNewOffsetsShouldSupportCommitInFuture() {
        final PartitionData pd = new PartitionData(null, createCursor(100L));
        final PartitionData.CommitResult cr = pd.onCommitOffset(createCursor(110L));

        assertEquals(0L, cr.committedCount);
        assertEquals(true, cr.seekOnKafka);
        assertEquals(110L, Long.parseLong(pd.getSentOffset().getOffset()));
        assertEquals(0L, pd.getUnconfirmed());
    }

    @Test
    public void normalOperationShouldNotReconfigureKafkaConsumer() {
        final PartitionData pd = new PartitionData(null, createCursor(100L));
        for (long i = 0; i < 100; ++i) {
            pd.addEvent(new ConsumedEvent("test_" + i, createCursor(100L + i + 1)));
        }
        // Now say to it that it was sent
        pd.takeEventsToStream(currentTimeMillis(), 1000, 0L);
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
        final PartitionData pd = new PartitionData(null, createCursor(100L));
        for (int i = 0; i < 100; ++i) {
            pd.takeEventsToStream(currentTimeMillis(), 10, 0L);
            assertEquals(i + 1, pd.getKeepAliveInARow());
        }
        pd.addEvent(new ConsumedEvent("", createCursor(101L)));
        assertEquals(100, pd.getKeepAliveInARow());
        pd.takeEventsToStream(currentTimeMillis(), 10, 0L);
        assertEquals(0, pd.getKeepAliveInARow());
        pd.takeEventsToStream(currentTimeMillis(), 10, 0L);
        assertEquals(1, pd.getKeepAliveInARow());
    }

    @Test
    public void eventsShouldBeStreamedOnTimeout() throws InterruptedException {
        final long timeout = TimeUnit.SECONDS.toMillis(1);
        final PartitionData pd = new PartitionData(null, createCursor(100L));
        for (int i = 0; i < 100; ++i) {
            pd.addEvent(new ConsumedEvent("test", createCursor(i + 100L + 1)));
        }
        List<ConsumedEvent> data = pd.takeEventsToStream(currentTimeMillis(), 1000, timeout);
        assertNull(data);
        assertEquals(0, pd.getKeepAliveInARow());
        Thread.sleep(timeout);

        data = pd.takeEventsToStream(currentTimeMillis(), 1000, timeout);
        assertNotNull(data);
        assertEquals(100, data.size());

        for (int i = 100; i < 200; ++i) {
            pd.addEvent(new ConsumedEvent("test", createCursor(i + 100L + 1)));
        }
        data = pd.takeEventsToStream(currentTimeMillis(), 1000, timeout);
        assertNull(data);
        assertEquals(0, pd.getKeepAliveInARow());
        Thread.sleep(timeout);

        data = pd.takeEventsToStream(currentTimeMillis(), 1000, timeout);
        assertNotNull(data);
        assertEquals(100, data.size());
    }

    @Test
    public void eventsShouldBeStreamedOnBatchSize() {
        final long timeout = TimeUnit.SECONDS.toMillis(1);
        final PartitionData pd = new PartitionData(null, createCursor(100L));
        for (int i = 0; i < 100; ++i) {
            pd.addEvent(new ConsumedEvent("test", createCursor(i + 100L + 1)));
        }
        assertNull(pd.takeEventsToStream(currentTimeMillis(), 1000, timeout));
        final List<ConsumedEvent> eventsToStream = pd.takeEventsToStream(currentTimeMillis(), 99, timeout);
        assertNotNull(eventsToStream);
        assertEquals(99, eventsToStream.size());
    }
}
