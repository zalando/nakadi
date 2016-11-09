package org.zalando.nakadi.repository.kafka;

import com.google.common.annotations.VisibleForTesting;
import com.netflix.hystrix.HystrixCommand;
import com.netflix.hystrix.HystrixCommandGroupKey;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.NetworkException;
import org.apache.kafka.common.errors.NotLeaderForPartitionException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zalando.nakadi.domain.BatchItem;
import org.zalando.nakadi.domain.EventPublishingStatus;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class ProducerSendCommand extends HystrixCommand<BatchItem> {

    private static final Logger LOG = LoggerFactory.getLogger(ProducerSendCommand.class);
    private static final String GROUP_PREFIX = "broker-";

    private final KafkaFactory kafkaFactory;
    private final String topicId;
    private final BatchItem batchItem;
    private final long timeout;

    protected ProducerSendCommand(final KafkaFactory kafkaFactory,
                                  final String topicId,
                                  final BatchItem batchItem,
                                  final long timeout) {
        super(HystrixCommandGroupKey.Factory.asKey(GROUP_PREFIX + batchItem.getBrokerId()), (int) timeout);
        this.kafkaFactory = kafkaFactory;
        this.topicId = topicId;
        this.batchItem = batchItem;
        this.timeout = timeout;
    }

    @Override
    protected BatchItem run() throws Exception {
        try {
            final ProducerRecord<String, String> kafkaRecord = new ProducerRecord<>(
                    topicId,
                    KafkaCursor.toKafkaPartition(batchItem.getPartition()),
                    batchItem.getPartition(),
                    batchItem.getEvent().toString());

            final Producer<String, String> producer = kafkaFactory.takeProducer();
            try {
                final NakadiCallback callback = new NakadiCallback();
                producer.send(kafkaRecord, callback);
                final Exception exception = callback.result.get(timeout, TimeUnit.MILLISECONDS);
                if (exception != null) {
                    if (isExceptionShouldLeadToReset(exception)) {
                        LOG.warn("Terminating producer while publishing to topic {}", topicId, exception);
                        kafkaFactory.terminateProducer(producer);
                    }

                    if (hasKafkaConnectionException(exception)) {
                        throw new Exception("Kafka timeout exception");
                    }
                }
            } finally {
                kafkaFactory.releaseProducer(producer);
            }
        } catch (final ExecutionException | RuntimeException ex) {
            batchItem.updateStatusAndDetail(EventPublishingStatus.FAILED, "internal error");
            LOG.error("Error publishing message to kafka", ex);
        } catch (final InterruptedException ex) {
            Thread.currentThread().interrupt();
            batchItem.updateStatusAndDetail(EventPublishingStatus.FAILED, "interrupted");
            LOG.error("Error publishing message to kafka", ex);
        } catch (final java.util.concurrent.TimeoutException ex) {
            batchItem.updateStatusAndDetail(EventPublishingStatus.FAILED, "timed out");
            LOG.error("Kafka timeout: {} on broker {}", topicId, batchItem.getBrokerId(), ex);
            throw new Exception("Kafka timeout exception");
        }

        return batchItem;
    }

    @Override
    protected BatchItem getFallback() {
        LOG.error("Fallback: Kafka timeout: {} on broker {}",
                topicId, batchItem.getBrokerId(), getExecutionException());
        batchItem.updateStatusAndDetail(EventPublishingStatus.FAILED, "timed out");
        return batchItem;
    }

    private boolean isExceptionShouldLeadToReset(final Exception exception) {
        return exception instanceof NotLeaderForPartitionException ||
                exception instanceof UnknownTopicOrPartitionException;
    }

    private boolean hasKafkaConnectionException(final Exception exception) {
        return exception instanceof TimeoutException ||
                exception instanceof NetworkException ||
                exception instanceof UnknownServerException;
    }

    @VisibleForTesting
    class NakadiCallback implements Callback {

        private CompletableFuture<Exception> result = new CompletableFuture<>();

        @Override
        public void onCompletion(final RecordMetadata metadata, final Exception exception) {
            if (null != exception) {
                LOG.warn("Failed to publish to kafka topic {}", topicId, exception);
                batchItem.updateStatusAndDetail(EventPublishingStatus.FAILED, "internal error");
                result.complete(exception);
            } else {
                batchItem.updateStatusAndDetail(EventPublishingStatus.SUBMITTED, "");
                result.complete(null);
            }
        }

        @VisibleForTesting
        void setResult(CompletableFuture<Exception> result) {
            this.result = result;
        }
    }
}
