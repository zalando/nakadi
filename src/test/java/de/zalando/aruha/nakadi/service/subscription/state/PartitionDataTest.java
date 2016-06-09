package de.zalando.aruha.nakadi.service.subscription.state;

import java.util.SortedMap;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.junit.Test;

public class PartitionDataTest {

    @Test
    public void onNewOffsetsShouldSupportRollback() {
        final PartitionData pd = new PartitionData(null, 100L);
        final PartitionData.CommitResult cr = pd.onCommitOffset(90L);

        Assert.assertEquals(0L, cr.commitedCount);
        Assert.assertEquals(true, cr.seekOnKafka);
        Assert.assertEquals(90L, pd.getSentOffset());
        Assert.assertEquals(0L, pd.getUnconfirmed());
    }

    @Test
    public void onNewOffsetsShouldSupportCommitInFuture() {
        final PartitionData pd = new PartitionData(null, 100L);
        final PartitionData.CommitResult cr = pd.onCommitOffset(110L);

        Assert.assertEquals(10L, cr.commitedCount);
        Assert.assertEquals(true, cr.seekOnKafka);
        Assert.assertEquals(110L, pd.getSentOffset());
        Assert.assertEquals(0L, pd.getUnconfirmed());
    }

    @Test
    public void normalOperationShouldNotReconfigureKafkaConsumer() {
        final PartitionData pd = new PartitionData(null, 100L);
        for (long i = 0; i < 100; ++i) {
            pd.addEvent(100L + i + 1, "test_" + i);
        }
        // Now say to it that it was sent
        pd.takeEventsToStream(System.currentTimeMillis(), 1000, 0L);
        Assert.assertEquals(100L, pd.getUnconfirmed());
        for (long i = 0; i < 10; ++i) {
            final PartitionData.CommitResult cr = pd.onCommitOffset(110L + i * 10L);
            Assert.assertEquals(10L, cr.commitedCount);
            Assert.assertFalse(cr.seekOnKafka);
            Assert.assertEquals(90L - i * 10L, pd.getUnconfirmed());
        }
    }

    @Test
    public void eventsMustBeReturnedInGuaranteedOrder() {
        final PartitionData pd = new PartitionData(null, 100L);
        for (long i = 0; i < 100; ++i) {
            pd.addEvent(200L - i, "test_" + (200L - i));
        }
        pd.addEvent(201L, "fake");
        for (int i = 0; i < 10; ++i) {
            final SortedMap<Long, String> data = pd.takeEventsToStream(System.currentTimeMillis(), 10, 0L);
            Assert.assertEquals(10, data.size());
            Assert.assertEquals((i + 1) * 10, pd.getUnconfirmed());
            Assert.assertEquals(0, pd.getKeepAlivesInARow());
            Assert.assertEquals(100L + i * 10L + 1L, data.firstKey().longValue());
            Assert.assertEquals(100L + i * 10L + 10L, data.lastKey().longValue());
            data.forEach((k, v) -> Assert.assertEquals("test_" + k, v));
        }
        final SortedMap<Long, String> data = pd.takeEventsToStream(System.currentTimeMillis(), 10, 0L);
        Assert.assertEquals(1, data.size());
        Assert.assertEquals(201L, data.firstKey().longValue());
        Assert.assertEquals("fake", data.get(data.firstKey()));
        Assert.assertEquals(0, pd.getKeepAlivesInARow());
    }

    @Test
    public void keepAliveCountShouldIncreaseOnEachEmptyCall() {
        final PartitionData pd = new PartitionData(null, 100L);
        for (int i = 0; i < 100; ++i) {
            pd.takeEventsToStream(System.currentTimeMillis(), 10, 0L);
            Assert.assertEquals(i + 1, pd.getKeepAlivesInARow());
        }
        pd.addEvent(101L, "");
        Assert.assertEquals(100, pd.getKeepAlivesInARow());
        pd.takeEventsToStream(System.currentTimeMillis(), 10, 0L);
        Assert.assertEquals(0, pd.getKeepAlivesInARow());
        pd.takeEventsToStream(System.currentTimeMillis(), 10, 0L);
        Assert.assertEquals(1, pd.getKeepAlivesInARow());
    }

    @Test
    public void eventsShouldBeStreamedOnTimeout() throws InterruptedException {
        final long timeout = TimeUnit.SECONDS.toMillis(1);
        final PartitionData pd = new PartitionData(null, 100L);
        for (int i = 0; i < 100; ++i) {
            pd.addEvent(i + 100L + 1, "test");
        }
        {
            final SortedMap<Long, String> data = pd.takeEventsToStream(System.currentTimeMillis(), 1000, timeout);
            Assert.assertNull(data);
            Assert.assertEquals(0, pd.getKeepAlivesInARow());
            Thread.sleep(timeout);
        }
        {
            final SortedMap<Long, String> data = pd.takeEventsToStream(System.currentTimeMillis(), 1000, timeout);
            Assert.assertNotNull(data);
            Assert.assertEquals(100, data.size());
        }
        for (int i = 100; i < 200; ++i) {
            pd.addEvent(i + 100L + 1, "test");
        }
        {
            final SortedMap<Long, String> data = pd.takeEventsToStream(System.currentTimeMillis(), 1000, timeout);
            Assert.assertNull(data);
            Assert.assertEquals(0, pd.getKeepAlivesInARow());
            Thread.sleep(timeout);
        }
        {
            final SortedMap<Long, String> data = pd.takeEventsToStream(System.currentTimeMillis(), 1000, timeout);
            Assert.assertNotNull(data);
            Assert.assertEquals(100, data.size());
        }
    }

    @Test
    public void eventsShouldBeStreamedOnBatchSize() {
        final long timeout = TimeUnit.SECONDS.toMillis(1);
        final PartitionData pd = new PartitionData(null, 100L);
        for (int i = 0; i < 100; ++i) {
            pd.addEvent(i + 100L + 1, "test");
        }
        Assert.assertNull(pd.takeEventsToStream(System.currentTimeMillis(), 1000, timeout));
        Assert.assertEquals(99, pd.takeEventsToStream(System.currentTimeMillis(), 99, timeout).size());
    }
}