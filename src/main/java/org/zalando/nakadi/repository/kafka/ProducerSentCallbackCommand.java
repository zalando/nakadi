package org.zalando.nakadi.repository.kafka;

import com.netflix.hystrix.HystrixCommand;
import com.netflix.hystrix.HystrixCommandGroupKey;
import com.netflix.hystrix.HystrixCommandKey;
import com.netflix.hystrix.HystrixCommandProperties;
import org.apache.kafka.common.errors.NetworkException;
import org.apache.kafka.common.errors.NotLeaderForPartitionException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zalando.nakadi.domain.BatchItem;
import org.zalando.nakadi.domain.EventPublishingStatus;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class ProducerSentCallbackCommand extends HystrixCommand<BatchItem> {

    private static final Logger LOG = LoggerFactory.getLogger(ProducerSentCallbackCommand.class);
    private static final String COMMAND_PREFIX = "broker-";

    private final ProducerSendCommand.NakadiCallback callback;
    private final Runnable producerTerminator;
    private final long timeout;

    protected ProducerSentCallbackCommand(final ProducerSendCommand.NakadiCallback callback,
                                          final Runnable producerTerminator,
                                          final long timeout) {
        super(Setter.withGroupKey(HystrixCommandGroupKey.Factory.asKey("kafka"))
                .andCommandKey(HystrixCommandKey.Factory.asKey(COMMAND_PREFIX + callback.getBatchItem().getBrokerId()))
                .andCommandPropertiesDefaults(
                        HystrixCommandProperties.Setter().withExecutionTimeoutInMilliseconds((int) timeout)));
        this.callback = callback;
        this.producerTerminator = producerTerminator;
        this.timeout = timeout;
    }

    @Override
    protected BatchItem run() throws Exception {
        final BatchItem batchItem = callback.getBatchItem();
        final String topic = batchItem.getTopic();
        try {
            LOG.debug("Waiting for callback from Kafka");
            callback.getResult().get(timeout, TimeUnit.MILLISECONDS);
            batchItem.updateStatusAndDetail(EventPublishingStatus.SUBMITTED, "");
        } catch (final ExecutionException ex) {
            LOG.error("Error publishing message to kafka", ex);
            batchItem.updateStatusAndDetail(EventPublishingStatus.FAILED, "internal error");

            if (hasKafkaConnectionException(ex.getCause())) {
                LOG.error("Kafka timeout: {} on broker {}", topic, batchItem.getBrokerId(), ex);
                batchItem.updateStatusAndDetail(EventPublishingStatus.FAILED, "timed out");
                throw new Exception("Kafka timeout exception");
            }

            if (isExceptionShouldLeadToReset(ex.getCause())) {
                LOG.warn("Terminating producer while publishing to topic {}", topic, ex);
                producerTerminator.run();
            }
        } catch (final InterruptedException ex) {
            Thread.currentThread().interrupt();
            LOG.error("Error publishing message to kafka", ex);
            batchItem.updateStatusAndDetail(EventPublishingStatus.FAILED, "interrupted");
        } catch (final java.util.concurrent.TimeoutException ex) {
            LOG.error("Kafka timeout: {} on broker {}", topic, batchItem.getBrokerId(), ex);
            batchItem.updateStatusAndDetail(EventPublishingStatus.FAILED, "timed out");
            throw new Exception("Kafka timeout exception");
        } catch (final Throwable th) {
            LOG.error("Error publishing message to kafka", th);
            batchItem.updateStatusAndDetail(EventPublishingStatus.FAILED, "internal error");
        }

        return batchItem;
    }

    private boolean isExceptionShouldLeadToReset(final Throwable exception) {
        return exception instanceof NotLeaderForPartitionException ||
                exception instanceof UnknownTopicOrPartitionException;
    }

    private boolean hasKafkaConnectionException(final Throwable exception) {
        return exception instanceof TimeoutException ||
                exception instanceof NetworkException ||
                exception instanceof UnknownServerException;
    }
}
