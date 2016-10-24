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
import org.zalando.nakadi.exceptions.EventPublishingException;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class KafkaSyncPostCommandTest {

    private TestProducer mockedKafkaProducer;
    private KafkaFactory kafkaFactory;

    @Before
    public void setUp() throws Exception {
        mockedKafkaProducer = new TestProducer();
        kafkaFactory = Mockito.mock(KafkaFactory.class);
        Mockito.when(kafkaFactory.takeProducer()).thenReturn(mockedKafkaProducer);
    }

    @Test
    public void testKafkaTimeoutException() {
        final BatchItem batchItem = new BatchItem(new JSONObject());
        batchItem.setPartition("0");
        mockedKafkaProducer.setException(new TimeoutException());

        try {
            createCommand(batchItem).execute();
            Assert.fail();
        } catch (final HystrixRuntimeException hre) {
            Assert.assertTrue(hre.getCause() instanceof EventPublishingException);
            Assert.assertTrue(batchItem.getResponse().getPublishingStatus() == EventPublishingStatus.FAILED);
        }
    }

    @Test
    public void testUnknownTopicOrPartitionException() {
        final BatchItem batchItem = new BatchItem(new JSONObject());
        batchItem.setPartition("0");
        mockedKafkaProducer.setException(new UnknownTopicOrPartitionException());

        final KafkaSyncPostCommand.CommandResult result = createCommand(batchItem).execute();

        Assert.assertTrue(result.isFailed());
        Assert.assertTrue(result.getException() instanceof EventPublishingException);
        Assert.assertTrue(batchItem.getResponse().getPublishingStatus() == EventPublishingStatus.FAILED);
    }

    private KafkaSyncPostCommand createCommand(final BatchItem batchItem) {
        return new KafkaSyncPostCommand("topic-1",
                Collections.singletonList(batchItem),
                kafkaFactory,
                10000,
                5000);
    }

    private class TestProducer implements Producer<String, String> {

        private Exception exception;

        public void setException(final Exception exception) {
            this.exception = exception;
        }

        @Override
        public Future<RecordMetadata> send(final ProducerRecord<String, String> record) {
            return null;
        }

        @Override
        public Future<RecordMetadata> send(final ProducerRecord<String, String> record, final Callback callback) {
            callback.onCompletion(null, exception);
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