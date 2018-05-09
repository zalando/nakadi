package org.zalando.nakadi.service.subscription;

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
import org.zalando.nakadi.exceptions.ErrorGettingCursorTimeLagException;
import org.zalando.nakadi.exceptions.InvalidCursorException;
import org.zalando.nakadi.exceptions.NakadiException;
import org.zalando.nakadi.exceptions.runtime.MyNakadiRuntimeException1;
import org.zalando.nakadi.repository.EventConsumer;
import org.zalando.nakadi.repository.db.EventTypeCache;
import org.zalando.nakadi.service.NakadiCursorComparator;
import org.zalando.nakadi.service.timeline.TimelineService;
import org.zalando.nakadi.util.TimeLogger;

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

    private static final Logger LOG = LoggerFactory.getLogger(SubscriptionTimeLagService.class);


    private static final int EVENT_FETCH_WAIT_TIME_MS = 1000;
    private static final int COMPLETE_TIMEOUT_MS = 60000;

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
            throws ErrorGettingCursorTimeLagException {

        TimeLogger.startMeasure("TIME_LAG", "putting of execution");

        final ExecutorService executor = Executors.newFixedThreadPool(10);

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
                LOG.info("[PARTITION_TIME_LAG] short path for " + cursor.getEventType() + ":" + cursor.getPartition());
                result.put(partition, Duration.ZERO);
            } else {
                futures.put(partition, executor.submit(() -> getNextEventTimeLag(cursor)));
            }
        }

        TimeLogger.addMeasure("shutdown");

        executor.shutdown();

        TimeLogger.addMeasure("waiting to finish");

        try {
            final boolean finished = executor.awaitTermination(COMPLETE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            if (!finished) {
                throw new MyNakadiRuntimeException1("timeout!!!");
            }

            TimeLogger.addMeasure("extracting results from futures");

            futures.forEach((partition, future) -> {
                try {
                    result.put(partition, future.get());
                } catch (InterruptedException | ExecutionException e) {
                    throw new MyNakadiRuntimeException1("wtf!!!");
                }
            });

            LOG.info(TimeLogger.finishMeasure());

            return result;

        } catch (InterruptedException e) {
            throw new MyNakadiRuntimeException1("interrupted!!!");
        }
    }

    private Duration getNextEventTimeLag(final NakadiCursor cursor) throws ErrorGettingCursorTimeLagException {
        final long start = System.currentTimeMillis();
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

            final long diff = System.currentTimeMillis() - start;
            LOG.info("[PARTITION_TIME_LAG] " + cursor.getEventType() + ":" + cursor.getPartition() + " " + diff);

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
