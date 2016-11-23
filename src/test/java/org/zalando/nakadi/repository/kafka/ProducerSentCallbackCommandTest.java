package org.zalando.nakadi.repository.kafka;

import com.netflix.hystrix.exception.HystrixRuntimeException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.zalando.nakadi.domain.BatchItem;
import org.zalando.nakadi.domain.EventPublishingStatus;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class ProducerSentCallbackCommandTest {

    @Test
    public void testKafkaTimeoutException() throws Exception {
        final BatchItem batchItem = new BatchItem(new JSONObject());
        batchItem.setPartition("0");

        final CompletableFuture cf = Mockito.mock(CompletableFuture.class);
        Mockito.when(cf.get()).thenThrow(new ExecutionException("", new TimeoutException()));
        final ProducerSendCommand.NakadiCallback callback = new ProducerSendCommand.NakadiCallback(batchItem);
        callback.setResult(cf);

        try {
            new ProducerSentCallbackCommand(callback, () -> {}, 10000).execute();
            Assert.fail();
        } catch (final HystrixRuntimeException hre) {
            Assert.assertTrue(batchItem.getResponse().getPublishingStatus() == EventPublishingStatus.FAILED);
            Assert.assertTrue(batchItem.getResponse().getDetail().equals("timed out"));
        }
    }

    @Test
    public void testHystrixTimeoutException() throws Exception {
        final BatchItem batchItem = new BatchItem(new JSONObject());
        batchItem.setPartition("0");

        final ProducerSendCommand.NakadiCallback callback = new ProducerSendCommand.NakadiCallback(batchItem);

        try {
            new ProducerSentCallbackCommand(callback, () -> {}, 1).execute();
            Assert.fail();
        } catch (final HystrixRuntimeException hre) {
            hre.printStackTrace();
            Assert.assertTrue(batchItem.getResponse().getPublishingStatus() == EventPublishingStatus.FAILED);
        }
    }

    @Test
    public void testUnknownTopicOrPartitionException() throws Exception {
        final BatchItem batchItem = new BatchItem(new JSONObject());
        batchItem.setPartition("0");

        final CompletableFuture cf = Mockito.mock(CompletableFuture.class);
        Mockito.when(cf.get()).thenThrow(new ExecutionException("", new UnknownTopicOrPartitionException()));
        final ProducerSendCommand.NakadiCallback callback = new ProducerSendCommand.NakadiCallback(batchItem);
        callback.setResult(cf);

        new ProducerSentCallbackCommand(callback, () -> {}, 10000).execute();
        Assert.assertTrue(batchItem.getResponse().getPublishingStatus() == EventPublishingStatus.FAILED);
        Assert.assertTrue(batchItem.getResponse().getDetail().equals("internal error"));
    }

}
