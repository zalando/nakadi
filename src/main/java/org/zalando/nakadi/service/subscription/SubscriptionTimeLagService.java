package org.zalando.nakadi.service.subscription;

import com.google.common.collect.ImmutableMap;
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
import org.zalando.nakadi.exceptions.runtime.InconsistentStateException;
import org.zalando.nakadi.repository.EventConsumer;
import org.zalando.nakadi.service.NakadiCursorComparator;
import org.zalando.nakadi.service.timeline.TimelineService;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Component
public class SubscriptionTimeLagService {

    private static final Logger LOG = LoggerFactory.getLogger(SubscriptionTimeLagService.class);

    private static final Map<String, Object> CUSTOM_CONSUMER_PROPERTIES
            = ImmutableMap.of("max.partition.fetch.bytes", 1);

    private static final int EVENT_FETCH_WAIT_TIME_MS = 10000;
    private static final int COMPLETE_TIMEOUT_MS = 30000;
    private static final int LAG_CALCULATION_PARALLELISM = 10;

    private final TimelineService timelineService;
    private final NakadiCursorComparator cursorComparator;

    @Autowired
    public SubscriptionTimeLagService(final TimelineService timelineService,
                                      final NakadiCursorComparator cursorComparator) {
        this.timelineService = timelineService;
        this.cursorComparator = cursorComparator;
    }

    public Map<EventTypePartition, Duration> getTimeLags(final Collection<NakadiCursor> committedPositions,
                                                         final List<PartitionEndStatistics> endPositions)
            throws ErrorGettingCursorTimeLagException, InconsistentStateException {

//        final ExecutorService executor = Executors.newFixedThreadPool(LAG_CALCULATION_PARALLELISM);

//        final Map<EventTypePartition, Future<Duration>> futures = new HashMap<>();
        final Map<EventTypePartition, Duration> result = new HashMap<>();

        final List<NakadiCursor> cursorsToProcess = new ArrayList<>();

        for (final NakadiCursor cursor : committedPositions) {
            final EventTypePartition partition = new EventTypePartition(cursor.getTimeline().getEventType(),
                    cursor.getPartition());
            if (isCursorAtTail(cursor, endPositions)) {
                result.put(partition, Duration.ZERO);
            } else {
                cursorsToProcess.add(cursor);
            }
        }

        final Map<EventTypePartition, Duration> nextEventTimeLags = getNextEventTimeLag(cursorsToProcess);
        result.putAll(nextEventTimeLags);
        return result;

//        executor.shutdown();
//        try {
//            final boolean finished = executor.awaitTermination(COMPLETE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
//            if (!finished) {
//                throw new InconsistentStateException("Timeout occurred when getting subscription time lag");
//            }
//            for (final EventTypePartition partition : futures.keySet()) {
//                final Future<Duration> future = futures.get(partition);
//                try {
//                    result.put(partition, future.get());
//                } catch (final ExecutionException e) {
//                    throw e.getCause();
//                }
//            }
//            return result;
//
//        } catch (final InconsistentStateException | ErrorGettingCursorTimeLagException e) {
//            throw e;
//        } catch (final Throwable e) {
//            throw new InconsistentStateException("Unexpected error occurred when getting subscription time lag", e);
//        }
    }

    private Map<EventTypePartition, Duration> getNextEventTimeLag(final List<NakadiCursor> cursors)
            throws ErrorGettingCursorTimeLagException, InconsistentStateException {
        if (cursors.isEmpty()) {
            return ImmutableMap.of();
        }

        try (final EventConsumer consumer = timelineService.createEventConsumer(CUSTOM_CONSUMER_PROPERTIES, cursors)){
            LOG.info("[TIME_LAG_FIX] starting consumer for {} partitions", cursors.size());

            boolean hasEventsFromAllPartition;
            final long startTime = System.currentTimeMillis();
            Map<EventTypePartition, Long> timestamps = new HashMap<>();
            do {
                final List<ConsumedEvent> events = consumer.readEvents();
                events.forEach(event -> {
                    final EventTypePartition partition = new EventTypePartition(event.getPosition().getEventType(),
                            event.getPosition().getEventType());
                    LOG.info("[TIME_LAG_FIX] Got event from partition: {}", partition);
                    if (!timestamps.containsKey(partition)) {
                        timestamps.put(partition, event.getTimestamp());
                    }
                });
                hasEventsFromAllPartition = timestamps.keySet().size() >= cursors.size();

            } while (System.currentTimeMillis() - startTime < EVENT_FETCH_WAIT_TIME_MS && !hasEventsFromAllPartition);

            LOG.info("[TIME_LAG_FIX] Got events from all partitions");

            return timestamps.entrySet().stream().collect(Collectors.toMap(
                    Map.Entry::getKey,
                    e -> Duration.ofMillis(new Date().getTime() - e.getValue())
            ));

        } catch (final NakadiException | IOException e) {
            throw new InconsistentStateException("Unexpected error happened when getting consumer time lag", e);
        } catch (final InvalidCursorException e) {
            throw new ErrorGettingCursorTimeLagException(cursors.get(0), e);
        }
    }

//    private Duration getNextEventTimeLag(final NakadiCursor cursor) throws ErrorGettingCursorTimeLagException,
//            InconsistentStateException {
//
//        try (final EventConsumer consumer = timelineService.createEventConsumer(CUSTOM_CONSUMER_PROPERTIES,
//                ImmutableList.of(cursor))){
//
//            final ConsumedEvent nextEvent = executeWithRetry(
//                    () -> {
//                        final List<ConsumedEvent> events = consumer.readEvents();
//                        return events.isEmpty() ? null : events.iterator().next();
//                    },
//                    new RetryForSpecifiedTimeStrategy<ConsumedEvent>(EVENT_FETCH_WAIT_TIME_MS)
//                            .withResultsThatForceRetry((ConsumedEvent) null));
//
//            if (nextEvent == null) {
//                return Duration.ZERO;
//            } else {
//                return Duration.ofMillis(new Date().getTime() - nextEvent.getTimestamp());
//            }
//        } catch (final NakadiException | IOException e) {
//            throw new InconsistentStateException("Unexpected error happened when getting consumer time lag", e);
//        } catch (final InvalidCursorException e) {
//            throw new ErrorGettingCursorTimeLagException(cursor, e);
//        }
//    }

    private boolean isCursorAtTail(final NakadiCursor cursor, final List<PartitionEndStatistics> endPositions) {
        return endPositions.stream()
                .map(PartitionEndStatistics::getLast)
                .filter(last -> last.getEventType().equals(cursor.getEventType())
                        && last.getPartition().equals(cursor.getPartition()))
                .findAny()
                .map(last -> cursorComparator.compare(cursor, last) >= 0)
                .orElse(false);
    }

}
