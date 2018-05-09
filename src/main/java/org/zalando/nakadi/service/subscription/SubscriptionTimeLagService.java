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
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.echocat.jomon.runtime.concurrent.Retryer.executeWithRetry;

@Component
public class SubscriptionTimeLagService {

    private static final int EVENT_FETCH_WAIT_TIME_MS = 1000;
    private static final int COMPLETE_TIMEOUT_MS = 60000;

    private final TimelineService timelineService;
    private final EventTypeCache eventTypeCache;

    @Autowired
    public SubscriptionTimeLagService(final TimelineService timelineService,
                                      final EventTypeCache eventTypeCache) {
        this.timelineService = timelineService;
        this.eventTypeCache = eventTypeCache;
    }

    public Map<EventTypePartition, Duration> getTimeLags(final Collection<NakadiCursor> committedPositions,
                                                         final List<PartitionEndStatistics> endPositions)
            throws ErrorGettingCursorTimeLagException {

        final NakadiCursorComparator cursorComparator = new NakadiCursorComparator(eventTypeCache);

        final ThreadPoolExecutor executor = new ThreadPoolExecutor(0, 10, 100, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>());

        Map<EventTypePartition, Future<Duration>> futures = new HashMap<>();
        Map<EventTypePartition, Duration> result = new HashMap<>();

        for (NakadiCursor c : committedPositions) {
            final EventTypePartition etp = new EventTypePartition(c.getTimeline().getEventType(), c.getPartition());
            final boolean isAtTail = endPositions.stream()
                    .filter(endStats -> endStats.getLast().getEventType().equals(c.getEventType())
                            && endStats.getLast().getPartition().equals(c.getPartition()))
                    .findAny()
                    .map(endPos -> cursorComparator.compare(c, endPos.getLast()) >= 0)
                    .orElse(false);

            if (isAtTail) {
                result.put(etp, Duration.ZERO);
            } else {
                futures.put(etp, executor.submit(() -> getNextEventTimeLag(c)));
            }
        }

        executor.shutdown();
        try {
            final boolean finished = executor.awaitTermination(COMPLETE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            if (!finished) {
                throw new MyNakadiRuntimeException1("timeout!!!");
            }
            futures.forEach((partition, future) -> {
                try {
                    result.put(partition, future.get());
                } catch (InterruptedException | ExecutionException e) {
                    throw new MyNakadiRuntimeException1("wtf!!!");
                }
            });
            return result;

        } catch (InterruptedException e) {
            throw new MyNakadiRuntimeException1("interrupted!!!");
        }
    }

    private Duration getNextEventTimeLag(final NakadiCursor cursor) throws ErrorGettingCursorTimeLagException {
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
        } catch (final NakadiException | InvalidCursorException e) {
            throw new ErrorGettingCursorTimeLagException(cursor, e);
        }
    }

}
