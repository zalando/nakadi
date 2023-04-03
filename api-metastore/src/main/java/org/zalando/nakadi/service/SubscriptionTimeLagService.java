package org.zalando.nakadi.service;

import com.google.common.collect.ImmutableList;
import org.echocat.jomon.runtime.concurrent.RetryForSpecifiedTimeStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.domain.ConsumedEvent;
import org.zalando.nakadi.domain.EventTypePartition;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.PartitionEndStatistics;
import org.zalando.nakadi.exceptions.runtime.ErrorGettingCursorTimeLagException;
import org.zalando.nakadi.exceptions.runtime.InconsistentStateException;
import org.zalando.nakadi.exceptions.runtime.InvalidCursorException;
import org.zalando.nakadi.repository.EventConsumer;
import org.zalando.nakadi.service.timeline.TimelineService;

import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.echocat.jomon.runtime.concurrent.Retryer.executeWithRetry;

@Component
public class SubscriptionTimeLagService {
    private static final Logger LOG = LoggerFactory.getLogger(SubscriptionTimeLagService.class);
    private static final int EVENT_FETCH_WAIT_TIME_MS = 1000;
    private static final int REQUEST_TIMEOUT_MS = 30000;
    private static final int MAX_THREADS_PER_REQUEST = 20;
    private static final int TIME_LAG_COMMON_POOL_SIZE = 400;

    private final TimelineService timelineService;
    private final NakadiCursorComparator cursorComparator;
    private final ThreadPoolExecutor threadPool;

    @Autowired
    public SubscriptionTimeLagService(final TimelineService timelineService,
                                      final NakadiCursorComparator cursorComparator) {
        this.timelineService = timelineService;
        this.cursorComparator = cursorComparator;
        this.threadPool = new ThreadPoolExecutor(0, TIME_LAG_COMMON_POOL_SIZE, 60L, TimeUnit.SECONDS,
                new SynchronousQueue<>());
    }

    public Map<EventTypePartition, Duration> getTimeLags(final Collection<NakadiCursor> committedPositions,
                                                         final List<PartitionEndStatistics> endPositions) {

        final TimeLagRequestHandler timeLagHandler = new TimeLagRequestHandler(timelineService, threadPool);
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
            CompletableFuture
                    .allOf(futureTimeLags.values().toArray(new CompletableFuture[futureTimeLags.size()]))
                    .get(timeLagHandler.getRemainingTimeoutMs(), TimeUnit.MILLISECONDS);

            for (final EventTypePartition partition : futureTimeLags.keySet()) {
                timeLags.put(partition, futureTimeLags.get(partition).get());
            }
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

        private final TimelineService timelineService;
        private final ThreadPoolExecutor threadPool;
        private final Semaphore semaphore;
        private final long timeoutTimestampMs;

        TimeLagRequestHandler(final TimelineService timelineService, final ThreadPoolExecutor threadPool) {
            this.timelineService = timelineService;
            this.threadPool = threadPool;
            this.semaphore = new Semaphore(MAX_THREADS_PER_REQUEST);
            this.timeoutTimestampMs = System.currentTimeMillis() + REQUEST_TIMEOUT_MS;
        }

        CompletableFuture<Duration> getCursorTimeLagFuture(final NakadiCursor cursor)
                throws InterruptedException, TimeoutException {

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

        private Duration getNextEventTimeLag(final NakadiCursor cursor) throws ErrorGettingCursorTimeLagException,
                InconsistentStateException {

            final String clientId = String.format("time-lag-checker-%s-%s",
                    cursor.getEventType(), cursor.getPartition());
            try (EventConsumer consumer = timelineService.createEventConsumer(clientId, ImmutableList.of(cursor))) {
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
            } catch (final IOException e) {
                throw new InconsistentStateException("Unexpected error happened when getting consumer time lag", e);
            } catch (final InvalidCursorException e) {
                throw new ErrorGettingCursorTimeLagException(cursor, e);
            } finally {
                LOG.trace("client:{}, finished reading events for lag calculation", clientId);
            }
        }
    }

}
