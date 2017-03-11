package org.zalando.nakadi.service.subscription.state;

import org.junit.Test;

import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

import static java.lang.System.currentTimeMillis;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class PartitionDataTest {

    @Test
    public void onNewOffsetsShouldSupportRollback() {
        final PartitionData pd = new PartitionData(null, 100L);
        final PartitionData.CommitResult cr = pd.onCommitOffset(90L);

        assertEquals(0L, cr.committedCount);
        assertEquals(true, cr.seekOnKafka);
        assertEquals(90L, pd.getSentOffset());
        assertEquals(0L, pd.getUnconfirmed());
    }

    @Test
    public void onNewOffsetsShouldSupportCommitInFuture() {
        final PartitionData pd = new PartitionData(null, 100L);
        final PartitionData.CommitResult cr = pd.onCommitOffset(110L);

        assertEquals(10L, cr.committedCount);
        assertEquals(true, cr.seekOnKafka);
        assertEquals(110L, pd.getSentOffset());
        assertEquals(0L, pd.getUnconfirmed());
    }

    @Test
    public void normalOperationShouldNotReconfigureKafkaConsumer() {
        final PartitionData pd = new PartitionData(null, 100L);
        for (long i = 0; i < 100; ++i) {
            pd.addEventFromKafka(100L + i + 1, ("test_" + i).getBytes());
        }
        // Now say to it that it was sent
        pd.takeEventsToStream(currentTimeMillis(), 1000, 0L);
        assertEquals(100L, pd.getUnconfirmed());
        for (long i = 0; i < 10; ++i) {
            final PartitionData.CommitResult cr = pd.onCommitOffset(110L + i * 10L);
            assertEquals(10L, cr.committedCount);
            assertFalse(cr.seekOnKafka);
            assertEquals(90L - i * 10L, pd.getUnconfirmed());
        }
    }

    @Test
    public void eventsMustBeReturnedInGuaranteedOrder() {
        final PartitionData pd = new PartitionData(null, 100L);
        for (long i = 0; i < 100; ++i) {
            pd.addEventFromKafka(200L - i, ("test_" + (200L - i)).getBytes());
        }
        pd.addEventFromKafka(201L, "fake".getBytes());
        for (int i = 0; i < 10; ++i) {
            final SortedMap<Long, byte[]> data = pd.takeEventsToStream(currentTimeMillis(), 10, 0L);
            assertNotNull(data);
            assertEquals(10, data.size());
            assertEquals((i + 1) * 10, pd.getUnconfirmed());
            assertEquals(0, pd.getKeepAliveInARow());
            assertEquals(100L + i * 10L + 1L, data.firstKey().longValue());
            assertEquals(100L + i * 10L + 10L, data.lastKey().longValue());
            data.forEach((k, v) -> assertArrayEquals(("test_" + k).getBytes(), v));
        }
        final SortedMap<Long, byte[]> data = pd.takeEventsToStream(currentTimeMillis(), 10, 0L);
        assertNotNull(data);
        assertEquals(1, data.size());
        assertEquals(201L, data.firstKey().longValue());
        assertArrayEquals("fake".getBytes(), data.get(data.firstKey()));
        assertEquals(0, pd.getKeepAliveInARow());
    }

    @Test
    public void keepAliveCountShouldIncreaseOnEachEmptyCall() {
        final PartitionData pd = new PartitionData(null, 100L);
        for (int i = 0; i < 100; ++i) {
            pd.takeEventsToStream(currentTimeMillis(), 10, 0L);
            assertEquals(i + 1, pd.getKeepAliveInARow());
        }
        pd.addEventFromKafka(101L, new byte[0]);
        assertEquals(100, pd.getKeepAliveInARow());
        pd.takeEventsToStream(currentTimeMillis(), 10, 0L);
        assertEquals(0, pd.getKeepAliveInARow());
        pd.takeEventsToStream(currentTimeMillis(), 10, 0L);
        assertEquals(1, pd.getKeepAliveInARow());
    }

    @Test
    public void eventsShouldBeStreamedOnTimeout() throws InterruptedException {
        final long timeout = TimeUnit.SECONDS.toMillis(1);
        final PartitionData pd = new PartitionData(null, 100L);
        for (int i = 0; i < 100; ++i) {
            pd.addEventFromKafka(i + 100L + 1, "test".getBytes());
        }
        SortedMap<Long, byte[]> data = pd.takeEventsToStream(currentTimeMillis(), 1000, timeout);
        assertNull(data);
        assertEquals(0, pd.getKeepAliveInARow());
        Thread.sleep(timeout);

        data = pd.takeEventsToStream(currentTimeMillis(), 1000, timeout);
        assertNotNull(data);
        assertEquals(100, data.size());

        for (int i = 100; i < 200; ++i) {
            pd.addEventFromKafka(i + 100L + 1, "test".getBytes());
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
        final PartitionData pd = new PartitionData(null, 100L);
        for (int i = 0; i < 100; ++i) {
            pd.addEventFromKafka(i + 100L + 1, "test".getBytes());
        }
        assertNull(pd.takeEventsToStream(currentTimeMillis(), 1000, timeout));
        final SortedMap<Long, byte[]> eventsToStream = pd.takeEventsToStream(currentTimeMillis(), 99, timeout);
        assertNotNull(eventsToStream);
        assertEquals(99, eventsToStream.size());
    }
}
