package org.zalando.nakadi.repository.kafka;

import com.google.common.annotations.VisibleForTesting;
import com.netflix.hystrix.HystrixCommand;
import com.netflix.hystrix.HystrixCommandGroupKey;
import com.netflix.hystrix.HystrixCommandKey;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zalando.nakadi.domain.BatchItem;
import org.zalando.nakadi.domain.EventPublishingStatus;

import javax.annotation.Nullable;
import java.util.concurrent.CompletableFuture;

public class ProducerSendCommand extends HystrixCommand<ProducerSendCommand.NakadiCallback> {

    private static final Logger LOG = LoggerFactory.getLogger(ProducerSendCommand.class);

    private final Producer producer;
    private final BatchItem batchItem;

    protected ProducerSendCommand(final Producer producer,
                                  final BatchItem batchItem) {
        super(Setter.withGroupKey(HystrixCommandGroupKey.Factory.asKey("kafka"))
                .andCommandKey(HystrixCommandKey.Factory.asKey("broker-" + batchItem.getBrokerId())));
        this.producer = producer;
        this.batchItem = batchItem;
    }

    @Override
    @Nullable
    protected NakadiCallback run() {
        final ProducerRecord<String, String> kafkaRecord = new ProducerRecord<>(
                batchItem.getTopic(),
                KafkaCursor.toKafkaPartition(batchItem.getPartition()),
                batchItem.getPartition(),
                batchItem.getEvent().toString());
        final NakadiCallback callback = new NakadiCallback(batchItem);
        try {
            producer.send(kafkaRecord, callback);
            LOG.debug("Sent command to Kafka");
            return callback;
        } catch (final RuntimeException ex) {
            LOG.error("Error publishing message to kafka", ex);
            batchItem.updateStatusAndDetail(EventPublishingStatus.FAILED, "internal error");
        }
        return null;
    }

    static class NakadiCallback implements Callback {

        private final BatchItem batchItem;
        private CompletableFuture<Exception> result;

        NakadiCallback(final BatchItem batchItem) {
            this.batchItem = batchItem;
            this.result = new CompletableFuture<>();
        }

        // called on producer thread
        @Override
        public void onCompletion(final RecordMetadata metadata, final Exception exception) {
            LOG.debug("Received callback from Kafka");
            if (null == exception) {
                result.complete(null);
            } else {
                LOG.debug("Exception in callback from Kafka", exception);
                result.completeExceptionally(exception);
            }
        }

        @VisibleForTesting
        void setResult(final CompletableFuture<Exception> result) {
            this.result = result;
        }

        BatchItem getBatchItem() {
            return batchItem;
        }

        CompletableFuture<Exception> getResult() {
            return result;
        }
    }
}
