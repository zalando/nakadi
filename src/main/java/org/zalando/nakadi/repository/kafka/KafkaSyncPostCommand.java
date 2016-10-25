package org.zalando.nakadi.repository.kafka;

import com.google.common.base.Preconditions;
import com.netflix.hystrix.HystrixCommand;
import com.netflix.hystrix.HystrixCommandGroupKey;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.NetworkException;
import org.apache.kafka.common.errors.NotLeaderForPartitionException;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zalando.nakadi.domain.BatchItem;
import org.zalando.nakadi.domain.EventPublishingStatus;
import org.zalando.nakadi.domain.EventPublishingStep;
import org.zalando.nakadi.exceptions.EventPublishingException;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Command is aimed to prevent calls in cases Kafka is not available.
 */
public class KafkaSyncPostCommand extends HystrixCommand<KafkaSyncPostCommand.CommandResult> {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaSyncPostCommand.class);
    private static final String GROUP_KEY_PREFIX = "kafka-sync-post-";

    private final String topicId;
    private final List<BatchItem> batch;
    private final KafkaFactory kafkaFactory;
    private final long sendTimeout;

    public KafkaSyncPostCommand(final String topicId,
                                final List<BatchItem> batch,
                                final KafkaFactory kafkaFactory,
                                final long sendTimeout,
                                final long hystrixCommandTimeoutDeltaMs) {
        super(HystrixCommandGroupKey.Factory.asKey(GROUP_KEY_PREFIX + topicId),
                (int) (sendTimeout + hystrixCommandTimeoutDeltaMs));
        this.topicId = topicId;
        this.batch = batch;
        this.kafkaFactory = kafkaFactory;
        this.sendTimeout = sendTimeout;
    }

    @Override
    protected CommandResult run() throws Exception {
        batch.forEach(item -> Preconditions.checkNotNull(
                item.getPartition(), "BatchItem partition can't be null at the moment of publishing!"));
        final Producer<String, String> producer = kafkaFactory.takeProducer();
        final Map<BatchItem, CompletableFuture<Exception>> sendFutures = new HashMap<>();
        try {
            for (final BatchItem item : batch) {
                item.setStep(EventPublishingStep.PUBLISHING);
                sendFutures.put(item, publishItem(producer, topicId, item));
            }
            final CompletableFuture<Void> multiFuture = CompletableFuture.allOf(
                    sendFutures.values().toArray(new CompletableFuture<?>[sendFutures.size()]));
            multiFuture.get(sendTimeout, TimeUnit.MILLISECONDS);

            // Now lets check for errors
            final Optional<Exception> needReset = sendFutures.entrySet().stream()
                    .filter(entry -> isExceptionShouldLeadToReset(entry.getValue().getNow(null)))
                    .map(entry -> entry.getValue().getNow(null))
                    .findAny();
            if (needReset.isPresent()) {
                LOG.info("Terminating producer while publishing to topic " + topicId +
                        " because of unrecoverable exception", needReset.get());
                kafkaFactory.terminateProducer(producer);
            }
        } catch (final TimeoutException ex) {
            failUnpublished(batch, "timed out");
            throw new EventPublishingException("Error publishing message to kafka", ex);
        } catch (final ExecutionException ex) {
            failUnpublished(batch, "internal error");
            throw new EventPublishingException("Error publishing message to kafka", ex);
        } catch (final InterruptedException ex) {
            Thread.currentThread().interrupt();
            failUnpublished(batch, "interrupted");
            throw new EventPublishingException("Error publishing message to kafka", ex);
        } finally {
            kafkaFactory.releaseProducer(producer);
        }

        return processExceptions(sendFutures);
    }

    private CommandResult processExceptions(final Map<BatchItem, CompletableFuture<Exception>> sendFutures)
            throws EventPublishingException {
        final List<Exception> exceptions = sendFutures.entrySet().stream()
                .map(entry -> entry.getValue().getNow(null))
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        if (!exceptions.isEmpty()) {
            LOG.error("Exceptions while publishing batches to kafka: {}", exceptions);
            failUnpublished(batch, "internal error");
            final EventPublishingException eventPublishingException =
                    new EventPublishingException("Error publishing message to kafka");
            if (hasKafkaConnectionException(exceptions)) {
                throw eventPublishingException;
            }
            return new CommandResult(eventPublishingException);
        }
        return new CommandResult();
    }

    private boolean hasKafkaConnectionException(final List<Exception> exceptions) {
        return exceptions.stream().anyMatch(ex ->
                ex instanceof org.apache.kafka.common.errors.TimeoutException ||
                ex instanceof NetworkException ||
                ex instanceof UnknownServerException);
    }

    private static boolean isExceptionShouldLeadToReset(@Nullable final Exception exception) {
        if (null == exception) {
            return false;
        }
        return Stream.of(NotLeaderForPartitionException.class, UnknownTopicOrPartitionException.class).
                filter(clazz -> clazz.isAssignableFrom(exception.getClass()))
                .findAny().isPresent();
    }

    private void failUnpublished(final List<BatchItem> batch, final String reason) {
        batch.stream()
                .filter(item -> item.getResponse().getPublishingStatus() != EventPublishingStatus.SUBMITTED)
                .forEach(item -> item.updateStatusAndDetail(EventPublishingStatus.FAILED, reason));
    }

    private static CompletableFuture<Exception> publishItem(
            final Producer<String, String> producer, final String topicId, final BatchItem item)
            throws EventPublishingException {
        try {
            final CompletableFuture<Exception> result = new CompletableFuture<>();
            final ProducerRecord<String, String> kafkaRecord = new ProducerRecord<>(
                    topicId,
                    KafkaCursor.toKafkaPartition(item.getPartition()),
                    item.getPartition(),
                    item.getEvent().toString());

            producer.send(kafkaRecord, ((metadata, exception) -> {
                if (null != exception) {
                    item.updateStatusAndDetail(EventPublishingStatus.FAILED, "internal error");
                    result.complete(exception);
                } else {
                    item.updateStatusAndDetail(EventPublishingStatus.SUBMITTED, "");
                    result.complete(null);
                }
            }));
            return result;
        } catch (final InterruptException e) {
            item.updateStatusAndDetail(EventPublishingStatus.FAILED, "internal error");
            throw new EventPublishingException("Error publishing message to kafka", e);
        } catch (final RuntimeException e) {
            item.updateStatusAndDetail(EventPublishingStatus.FAILED, "internal error");
            throw new EventPublishingException("Error publishing message to kafka", e);
        }
    }

    public static class CommandResult {

        private final EventPublishingException exception;

        public CommandResult(final EventPublishingException exception) {
            this.exception = exception;
        }

        public CommandResult() {
            this.exception = null;
        }

        public boolean isFailed() {
            return exception != null;
        }

        @Nullable
        public EventPublishingException getException() {
            return exception;
        }
    }

}