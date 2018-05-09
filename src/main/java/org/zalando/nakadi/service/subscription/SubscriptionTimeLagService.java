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
import org.zalando.nakadi.repository.MultiTimelineEventConsumer;
import org.zalando.nakadi.repository.db.EventTypeCache;
import org.zalando.nakadi.service.NakadiCursorComparator;
import org.zalando.nakadi.service.timeline.TimelineService;

import java.time.Duration;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.echocat.jomon.runtime.concurrent.Retryer.executeWithRetry;

@Component
public class SubscriptionTimeLagService {

    private static final int EVENT_FETCH_WAIT_TIME = 1000;

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
        final MultiTimelineEventConsumer consumer = timelineService.createMultiTimelineEventConsumer(
                "time-lag-checker-" + UUID.randomUUID().toString());

        return committedPositions.stream()
                .collect(Collectors.toMap(
                        c -> new EventTypePartition(c.getTimeline().getEventType(), c.getPartition()),
                        c -> {
                            final boolean isAtTail = endPositions.stream()
                                    .filter(endStats -> endStats.getLast().getEventType().equals(c.getEventType())
                                            && endStats.getLast().getPartition().equals(c.getPartition()))
                                    .findAny()
                                    .map(endPos -> cursorComparator.compare(c, endPos.getLast()) >= 0)
                                    .orElse(false);

                            if (isAtTail) {
                                return Duration.ZERO;
                            } else {
                                return getNextEventTimeLag(c, consumer);
                            }
                        }
                ));
    }

    private Duration getNextEventTimeLag(final NakadiCursor cursor, final MultiTimelineEventConsumer consumer)
            throws ErrorGettingCursorTimeLagException {
        try {
            consumer.reassign(ImmutableList.of(cursor));
        } catch (final NakadiException | InvalidCursorException e) {
            throw new ErrorGettingCursorTimeLagException(cursor, e);
        }

        final ConsumedEvent nextEvent = executeWithRetry(
                () -> {
                    final List<ConsumedEvent> events = consumer.readEvents();
                    return events.isEmpty() ? null : events.iterator().next();
                },
                new RetryForSpecifiedTimeStrategy<ConsumedEvent>(EVENT_FETCH_WAIT_TIME)
                        .withResultsThatForceRetry((ConsumedEvent) null));

        if (nextEvent == null) {
            return Duration.ZERO;
        } else {
            return Duration.ofMillis(new Date().getTime() - nextEvent.getTimestamp());
        }
    }

}
