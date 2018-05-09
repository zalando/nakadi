package org.zalando.nakadi.service.subscription;

import com.google.common.collect.ImmutableList;
import org.echocat.jomon.runtime.concurrent.RetryForSpecifiedTimeStrategy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.domain.ConsumedEvent;
import org.zalando.nakadi.domain.EventTypePartition;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.PartitionEndStatistics;
import org.zalando.nakadi.exceptions.ErrorGettingCursorTimeLagException;
import org.zalando.nakadi.exceptions.InvalidCursorException;
import org.zalando.nakadi.exceptions.NakadiException;
import org.zalando.nakadi.exceptions.runtime.InconsistentStateException;
import org.zalando.nakadi.exceptions.runtime.MyNakadiRuntimeException1;
import org.zalando.nakadi.repository.EventConsumer;
import org.zalando.nakadi.repository.db.EventTypeCache;
import org.zalando.nakadi.service.NakadiCursorComparator;
import org.zalando.nakadi.service.timeline.TimelineService;

import java.time.Duration;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.echocat.jomon.runtime.concurrent.Retryer.executeWithRetry;

@Component
public class SubscriptionTimeLagService {

    private static final int EVENT_FETCH_WAIT_TIME_MS = 1000;
    private static final int COMPLETE_TIMEOUT_MS = 30000;
    private static final int LAG_CALCULATION_PARALLELISM = 10;

    private final TimelineService timelineService;
    private final NakadiCursorComparator cursorComparator;

    @Autowired
    public SubscriptionTimeLagService(final TimelineService timelineService,
                                      final EventTypeCache eventTypeCache) {
        this.timelineService = timelineService;
        this.cursorComparator = new NakadiCursorComparator(eventTypeCache);
    }

    public Map<EventTypePartition, Duration> getTimeLags(final Collection<NakadiCursor> committedPositions,
                                                         final List<PartitionEndStatistics> endPositions)
            throws ErrorGettingCursorTimeLagException, InconsistentStateException {

        final ExecutorService executor = Executors.newFixedThreadPool(LAG_CALCULATION_PARALLELISM);

        Map<EventTypePartition, Future<Duration>> futures = new HashMap<>();
        Map<EventTypePartition, Duration> result = new HashMap<>();

        for (final NakadiCursor cursor : committedPositions) {
            final EventTypePartition partition = new EventTypePartition(cursor.getTimeline().getEventType(),
                    cursor.getPartition());
            final boolean isAtTail = endPositions.stream()
                    .map(PartitionEndStatistics::getLast)
                    .filter(last -> last.getEventType().equals(cursor.getEventType())
                            && last.getPartition().equals(cursor.getPartition()))
                    .findAny()
                    .map(last -> cursorComparator.compare(cursor, last) >= 0)
                    .orElse(false);

            if (isAtTail) {
                result.put(partition, Duration.ZERO);
            } else {
                futures.put(partition, executor.submit(() -> getNextEventTimeLag(cursor)));
            }
        }

        executor.shutdown();
        try {
            final boolean finished = executor.awaitTermination(COMPLETE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            if (!finished) {
                throw new InconsistentStateException("Timeout occurred when getting subscription time lag");
            }

            futures.forEach((partition, future) -> {
                try {
                    result.put(partition, future.get());
                } catch (final InterruptedException e) {
                    throw new InconsistentStateException("Thread interrupted when getting subscription time lag", e);
                } catch (final ExecutionException e) {
                    if (e.getCause() instanceof MyNakadiRuntimeException1) {
                        throw (MyNakadiRuntimeException1) e.getCause();
                    } else {
                        throw new InconsistentStateException("Unexpected error occurred when getting subscription " +
                                "time lag", e);
                    }
                }
            });
            return result;

        } catch (final InterruptedException e) {
            throw new InconsistentStateException("Thread interrupted when getting subscription time lag", e);
        }
    }

    private Duration getNextEventTimeLag(final NakadiCursor cursor) throws ErrorGettingCursorTimeLagException,
            InconsistentStateException {
        try {
            final EventConsumer consumer = timelineService.createEventConsumer(
                    "time-lag-checker-" + UUID.randomUUID().toString(), ImmutableList.of(cursor));

            final ConsumedEvent nextEvent = executeWithRetry(
                    () -> {
                        final List<ConsumedEvent> events = consumer.readEvents();
                        return events.isEmpty() ? null : events.iterator().next();
                    },
                    new RetryForSpecifiedTimeStrategy<ConsumedEvent>(EVENT_FETCH_WAIT_TIME_MS)
                            .withResultsThatForceRetry((ConsumedEvent) null));

            if (nextEvent == null) {
                return Duration.ZERO;
            } else {
                return Duration.ofMillis(new Date().getTime() - nextEvent.getTimestamp());
            }
        } catch (final NakadiException e) {
            throw new InconsistentStateException("Unexpected error happened when getting consumer time lag", e);
        } catch (final InvalidCursorException e) {
            throw new ErrorGettingCursorTimeLagException(cursor, e);
        }
    }

}
