package org.zalando.nakadi.service;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableList;
import org.echocat.jomon.runtime.concurrent.RetryForSpecifiedTimeStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.config.NakadiSettings;
import org.zalando.nakadi.domain.ConsumedEvent;
import org.zalando.nakadi.domain.EventTypePartition;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.PartitionEndStatistics;
import org.zalando.nakadi.exceptions.runtime.ErrorGettingCursorTimeLagException;
import org.zalando.nakadi.exceptions.runtime.InconsistentStateException;
import org.zalando.nakadi.exceptions.runtime.InvalidCursorException;
import org.zalando.nakadi.service.timeline.HighLevelConsumer;
import org.zalando.nakadi.service.timeline.TimelineService;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import static org.echocat.jomon.runtime.concurrent.Retryer.executeWithRetry;

@Component
public class SubscriptionTimeLagService {
    private static final Logger LOG = LoggerFactory.getLogger(SubscriptionTimeLagService.class);
    private static final int EVENT_FETCH_WAIT_TIME_MS = 1000;
    private static final int REQUEST_TIMEOUT_MS = 30000;
    private static final int MAX_THREADS_PER_REQUEST = 20;
    private static final int TIME_LAG_COMMON_POOL_SIZE = 400;

    private final NakadiCursorComparator cursorComparator;
    private final ThreadPoolExecutor threadPool;
    private final ConsumerPool consumerPool;

    @Autowired
    public SubscriptionTimeLagService(final TimelineService timelineService,
                                      final NakadiCursorComparator cursorComparator,
                                      final MetricRegistry metricRegistry,
                                      final NakadiSettings nakadiSettings) {
        if (nakadiSettings.getKafkaTimeLagCheckerConsumerPoolSize() <= 0) {
            this.threadPool = null;
            this.consumerPool = null;
        } else {
            this.threadPool = new ThreadPoolExecutor(0, TIME_LAG_COMMON_POOL_SIZE, 60L, TimeUnit.SECONDS,
                    new SynchronousQueue<>());
            this.consumerPool = new ConsumerPool(
                    nakadiSettings.getKafkaTimeLagCheckerConsumerPoolSize(),
                    () -> timelineService.createEventConsumer("timelag-checker"),
                    metricRegistry);
        }
        this.cursorComparator = cursorComparator;
    }

    public Map<EventTypePartition, Duration> getTimeLags(final Collection<NakadiCursor> committedPositions,
                                                         final List<PartitionEndStatistics> endPositions) {
        if (consumerPool == null) {
            throw new IllegalStateException("Consumer pool was not initialized, check pool config");
        }

        final TimeLagRequestHandler timeLagHandler = new TimeLagRequestHandler(threadPool, consumerPool);
        final Map<EventTypePartition, Duration> timeLags = new HashMap<>();
        final Map<EventTypePartition, CompletableFuture<Duration>> futureTimeLags = new HashMap<>();
        try {
            for (final NakadiCursor cursor : committedPositions) {
                if (isCursorAtTail(cursor, endPositions)) {
                    timeLags.put(cursor.getEventTypePartition(), Duration.ZERO);
                } else {
                    final CompletableFuture<Duration> timeLagFuture = timeLagHandler.getCursorTimeLagFuture(cursor);
                    futureTimeLags.put(cursor.getEventTypePartition(), timeLagFuture);
                }
            }

            LOG.trace("Waiting for timelag futures to complete");
            CompletableFuture
                    .allOf(futureTimeLags.values().toArray(new CompletableFuture[futureTimeLags.size()]))
                    .get(timeLagHandler.getRemainingTimeoutMs(), TimeUnit.MILLISECONDS);

            for (final EventTypePartition partition : futureTimeLags.keySet()) {
                timeLags.put(partition, futureTimeLags.get(partition).get());
            }
            LOG.trace("Timelag futures completed");
        } catch (RejectedExecutionException | TimeoutException | ExecutionException e) {
            LOG.warn("caught exception the timelag stats are not complete: {}", e.toString());
        } catch (Throwable e) {
            LOG.warn("caught throwable the timelag stats are not complete: {}", e.toString());
        }
        return timeLags;
    }

    private boolean isCursorAtTail(final NakadiCursor cursor, final List<PartitionEndStatistics> endPositions) {
        return endPositions.stream()
                .map(PartitionEndStatistics::getLast)
                .filter(last -> last.getEventType().equals(cursor.getEventType())
                        && last.getPartition().equals(cursor.getPartition()))
                .findAny()
                .map(last -> cursorComparator.compare(cursor, last) >= 0)
                .orElse(false);
    }

    private static class TimeLagRequestHandler {

        private final ConsumerPool consumerPool;
        private final ThreadPoolExecutor threadPool;
        private final Semaphore semaphore;
        private final long timeoutTimestampMs;

        TimeLagRequestHandler(final ThreadPoolExecutor threadPool,
                              final ConsumerPool consumerPool) {
            this.consumerPool = consumerPool;
            this.threadPool = threadPool;
            this.semaphore = new Semaphore(MAX_THREADS_PER_REQUEST);
            this.timeoutTimestampMs = System.currentTimeMillis() + REQUEST_TIMEOUT_MS;
        }

        CompletableFuture<Duration> getCursorTimeLagFuture(final NakadiCursor cursor)
                throws InterruptedException, TimeoutException {

            LOG.trace("try acquire semaphore");
            final CompletableFuture<Duration> future = new CompletableFuture<>();
            if (semaphore.tryAcquire(getRemainingTimeoutMs(), TimeUnit.MILLISECONDS)) {
                threadPool.submit(() -> {
                    try {
                        final Duration timeLag = getNextEventTimeLag(cursor);
                        future.complete(timeLag);
                    } catch (final Throwable e) {
                        future.completeExceptionally(e);
                    } finally {
                        semaphore.release();
                    }
                });
            } else {
                throw new TimeoutException("Partition time lag timeout exceeded");
            }
            return future;
        }

        long getRemainingTimeoutMs() {
            if (timeoutTimestampMs > System.currentTimeMillis()) {
                return timeoutTimestampMs - System.currentTimeMillis();
            } else {
                return 0;
            }
        }

        private Duration getNextEventTimeLag(final NakadiCursor cursor)
                throws ErrorGettingCursorTimeLagException, InconsistentStateException {

            final String clientId = String.format("time-lag-checker-%s-%s",
                    cursor.getEventType(), cursor.getPartition());
            final HighLevelConsumer consumer = consumerPool.takeConsumer(ImmutableList.of(cursor));
            try {
                LOG.trace("client:{}, reading events for lag calculation", clientId);
                final ConsumedEvent nextEvent = executeWithRetry(
                        () -> {
                            // We ignore per event authorization here, because we are not exposing any data.
                            final List<ConsumedEvent> events = consumer.readEvents();
                            return events.isEmpty() ? null : events.iterator().next();
                        },
                        new RetryForSpecifiedTimeStrategy<ConsumedEvent>(EVENT_FETCH_WAIT_TIME_MS)
                                .withResultsThatForceRetry((ConsumedEvent) null));

                if (nextEvent == null) {
                    throw new InconsistentStateException("Timeout waiting for events when getting consumer time lag");
                } else {
                    return Duration.ofMillis(new Date().getTime() - nextEvent.getTimestamp());
                }
            } catch (final InvalidCursorException e) {
                throw new ErrorGettingCursorTimeLagException(cursor, e);
            } finally {
                LOG.trace("client:{}, finished reading events for lag calculation", clientId);
                consumerPool.returnConsumer(consumer);
            }
        }
    }

    private static class ConsumerPool {
        private final BlockingQueue<HighLevelConsumer> pool;
        private final Meter consumerPoolTakeMeter;
        private final Meter consumerPoolReturnMeter;

        ConsumerPool(final int consumerPoolSize,
                     final Supplier<HighLevelConsumer> consumerCreator,
                     final MetricRegistry metricsRegistry) {
            LOG.info("Preparing timelag checker pool of {} multi-timeline consumers", consumerPoolSize);
            this.pool = new LinkedBlockingQueue(consumerPoolSize);
            for (int i = 0; i < consumerPoolSize; ++i) {
                this.pool.add(consumerCreator.get());
            }

            this.consumerPoolTakeMeter = metricsRegistry.meter("nakadi.timelag-consumer.taken");
            this.consumerPoolReturnMeter = metricsRegistry.meter("nakadi.timelag-consumer.returned");
        }

        private HighLevelConsumer takeConsumer(final List<NakadiCursor> cursors) {
            final HighLevelConsumer consumer;

            LOG.trace("Taking timelag consumer from the pool");
            try {
                consumer = pool.poll(30, TimeUnit.SECONDS);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("interrupted while waiting for a consumer from the pool");
            }
            if (consumer == null) {
                throw new RuntimeException("timed out while waiting for a consumer from the pool");
            }

            LOG.trace("Took timelag consumer from the pool {}", consumer);

            consumer.reassign(cursors);
            LOG.trace("Reassigned consumer {} to cursors {}", consumer, cursors);

            consumerPoolTakeMeter.mark();

            return consumer;
        }

        private void returnConsumer(final HighLevelConsumer consumer) {
            LOG.trace("Returning timelag consumer to the pool {}", consumer);

            consumer.reassign(Collections.emptyList());
            LOG.trace("Reassigned consumer {} to empty cursors", consumer);

            consumerPoolReturnMeter.mark();

            try {
                pool.put(consumer);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("interrupted while putting a consumer back to the pool");
            }

            LOG.trace("Returned timelag consumer to the pool {}", consumer);
        }

    }

}
