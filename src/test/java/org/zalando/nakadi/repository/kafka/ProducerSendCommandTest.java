package org.zalando.nakadi.repository.kafka;

import com.netflix.hystrix.exception.HystrixRuntimeException;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.zalando.nakadi.domain.BatchItem;
import org.zalando.nakadi.domain.EventPublishingStatus;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class ProducerSendCommandTest {

    private TestProducer kafkaProducer;
    private KafkaFactory kafkaFactory;

    @Before
    public void setUp() throws Exception {
        kafkaProducer = new TestProducer();
        kafkaFactory = Mockito.mock(KafkaFactory.class);
        Mockito.when(kafkaFactory.takeProducer()).thenReturn(kafkaProducer);
    }

    @Test
    public void testGetFutureTimeoutException() throws Exception {
        final BatchItem batchItem = new BatchItem(new JSONObject());
        batchItem.setPartition("0");

        final Producer producer = Mockito.mock(Producer.class);
        Mockito.when(kafkaFactory.takeProducer()).thenReturn(producer);
        final CompletableFuture cf = Mockito.mock(CompletableFuture.class);
        Mockito.when(cf.get(10000, TimeUnit.MILLISECONDS)).thenThrow(new java.util.concurrent.TimeoutException());
        Mockito.when(producer.send(Mockito.any(), Mockito.any())).thenAnswer(invocation -> {
            final NakadiCallback callback =
                    (NakadiCallback) invocation.getArguments()[1];
            callback.setResult(cf);
            return null;
        });

        try {
            new ProducerSendCommand(kafkaFactory, "my-topic-1", batchItem, 10000).execute();
        } catch (final HystrixRuntimeException hre) {
            Assert.assertTrue(batchItem.getResponse().getPublishingStatus() == EventPublishingStatus.FAILED);
            Assert.assertTrue(batchItem.getResponse().getDetail().equals("timed out"));
        }
    }

    @Test
    public void testKafkaTimeoutException() throws Exception {
        final BatchItem batchItem = new BatchItem(new JSONObject());
        batchItem.setPartition("0");
        kafkaProducer.setException(new TimeoutException());

        try {
            new ProducerSendCommand(kafkaFactory, "my-topic-1", batchItem, 10000).execute();
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
        kafkaProducer.setWaitOnSend(true).setUseCallback(false);

        try {
            new ProducerSendCommand(kafkaFactory, "my-topic-1", batchItem, 100).execute();
            Assert.fail();
        } catch (final HystrixRuntimeException hre) {
            Assert.assertTrue(batchItem.getResponse().getPublishingStatus() == EventPublishingStatus.ABORTED);
        }
    }

    @Test
    public void testUnknownTopicOrPartitionException() throws Exception {
        final BatchItem batchItem = new BatchItem(new JSONObject());
        batchItem.setPartition("0");
        kafkaProducer.setException(new UnknownTopicOrPartitionException());

        new ProducerSendCommand(kafkaFactory, "my-topic-1", batchItem, 10000).execute();
        Assert.assertTrue(batchItem.getResponse().getPublishingStatus() == EventPublishingStatus.FAILED);
        Assert.assertTrue(batchItem.getResponse().getDetail().equals("internal error"));
    }

    private class TestProducer implements Producer<String, String> {

        private Exception exception;
        private boolean waitOnSend;
        private boolean useCallback = true;

        public TestProducer setException(final Exception exception) {
            this.exception = exception;
            return this;
        }

        public TestProducer setWaitOnSend(final boolean waitOnSend) {
            this.waitOnSend = waitOnSend;
            return this;
        }

        public TestProducer setUseCallback(final boolean useCallback) {
            this.useCallback = useCallback;
            return this;
        }

        @Override
        public Future<RecordMetadata> send(final ProducerRecord<String, String> record) {
            return null;
        }

        @Override
        public Future<RecordMetadata> send(final ProducerRecord<String, String> record, final Callback callback) {
            if (waitOnSend) {
                try {
                    Thread.sleep(20000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            if (useCallback) {
                callback.onCompletion(null, exception);
            }
            return null;
        }

        @Override
        public void flush() {
            // intentionally left empty for testing purposes
        }

        @Override
        public List<PartitionInfo> partitionsFor(final String topic) {
            return null;
        }

        @Override
        public Map<MetricName, ? extends Metric> metrics() {
            return null;
        }

        @Override
        public void close() {
            // intentionally left empty for testing purposes
        }

        @Override
        public void close(final long timeout, final TimeUnit unit) {
            // intentionally left empty for testing purposes
        }
    }
}