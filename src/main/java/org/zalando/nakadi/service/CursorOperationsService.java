package org.zalando.nakadi.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.NakadiCursorDistanceQuery;
import org.zalando.nakadi.domain.NakadiCursorDistanceResult;
import org.zalando.nakadi.domain.ShiftedNakadiCursor;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.exceptions.NakadiException;
import org.zalando.nakadi.exceptions.runtime.InvalidCursorOperation;
import org.zalando.nakadi.repository.TopicRepository;
import org.zalando.nakadi.service.timeline.TimelineService;

import java.util.List;
import java.util.stream.Collectors;

import static org.zalando.nakadi.exceptions.runtime.InvalidCursorOperation.Reason.CURSORS_WITH_DIFFERENT_PARTITION;
import static org.zalando.nakadi.exceptions.runtime.InvalidCursorOperation.Reason.INVERTED_OFFSET_ORDER;
import static org.zalando.nakadi.exceptions.runtime.InvalidCursorOperation.Reason.INVERTED_TIMELINE_ORDER;
import static org.zalando.nakadi.exceptions.runtime.InvalidCursorOperation.Reason.TIMELINE_NOT_FOUND;

@Service
public class CursorOperationsService {
    private final TimelineService timelineService;

    @Autowired
    public CursorOperationsService(final TimelineService timelineService) {
        this.timelineService = timelineService;
    }

    public List<NakadiCursorDistanceResult> calculateDistance(final List<NakadiCursorDistanceQuery> cursors)
        throws InvalidCursorOperation {
        return cursors.stream().map(this::calculateDistance).collect(Collectors.toList());
    }

    public NakadiCursorDistanceResult calculateDistance(final NakadiCursorDistanceQuery query)
            throws InvalidCursorOperation {
        final NakadiCursor initialCursor = query.getInitialCursor();
        final NakadiCursor finalCursor = query.getFinalCursor();

        // Validate query
        if (!initialCursor.getPartition().equals(finalCursor.getPartition())) {
            throw new InvalidCursorOperation(CURSORS_WITH_DIFFERENT_PARTITION);
        } else if (initialCursor.getTimeline().getOrder() > finalCursor.getTimeline().getOrder()) {
            throw new InvalidCursorOperation(INVERTED_TIMELINE_ORDER);
        }

        if (initialCursor.getTimeline().getOrder().equals(finalCursor.getTimeline().getOrder())) {
            final long distance = getDistanceSameTimeline(initialCursor, finalCursor);
            return new NakadiCursorDistanceResult(query, distance);
        } else {
            final long distance = getDistanceDifferentTimelines(initialCursor, finalCursor);
            return new NakadiCursorDistanceResult(query, distance);
        }
    }

    private long getDistanceSameTimeline(final NakadiCursor initialCursor, final NakadiCursor finalCursor) {
        final long distance = numberOfEventsBeforeCursor(finalCursor) - numberOfEventsBeforeCursor(initialCursor);
        if (distance < 0) {
            throw new InvalidCursorOperation(INVERTED_OFFSET_ORDER);
        }
        return distance;
    }

    private long getDistanceDifferentTimelines(final NakadiCursor initialCursor, final NakadiCursor finalCursor) {
        long distance = 0;

        // get distance from initialCursor to the end of the timeline
        distance += totalEventsInTimeline(initialCursor) - numberOfEventsBeforeCursor(initialCursor);

        // get all intermediary timelines sizes
        final String partitionString = initialCursor.getPartition();
        for (int order = initialCursor.getTimeline().getOrder() + 1;
             order < finalCursor.getTimeline().getOrder();
             order++) {
            final Timeline timeline = getTimeline(initialCursor.getTopic(), order);
            distance += totalEventsInTimeline(timeline, partitionString);
        }

        // do not check if the offset exists because there is no reason to do it.
        distance += numberOfEventsBeforeCursor(finalCursor);

        return distance;
    }

    public NakadiCursor unshiftCursor(final ShiftedNakadiCursor cursor) throws InvalidCursorOperation {
        if (cursor.getShift() < 0) {
            return moveCursorBackwards(cursor.getTopic(), cursor.getTimeline(), cursor.getPartition(),
                    numberOfEventsBeforeCursor(cursor),
                    cursor.getShift());
        } else {
            return moveCursorForward(cursor.getTopic(), cursor.getTimeline(), cursor.getPartition(),
                    numberOfEventsBeforeCursor(cursor), cursor.getShift());
        }
    }

    private NakadiCursor moveCursorBackwards(final String topic, final Timeline timeline, final String partition,
                                             final long offset, final long shift) {
        final long shiftedOffset = offset + shift;
        if (shiftedOffset >= 0) { // move left in the same timeline
            final String paddedOffset = getOffsetForPosition(timeline, shiftedOffset);
            return new NakadiCursor(timeline, partition, paddedOffset);
        } else { // move to previous timeline
            final Timeline previousTimeline = getTimeline(topic, timeline.getOrder() - 1);
            final long totalEventsInTimeline = totalEventsInTimeline(previousTimeline, partition);
            return moveCursorBackwards(topic, previousTimeline, partition, totalEventsInTimeline, shift + offset);
        }
    }

    private NakadiCursor moveCursorForward(final String topic, final Timeline timeline, final String partition,
                                    final long offset, final long shift) {
        if (!timeline.isActive() && !(offset + shift < totalEventsInTimeline(timeline, partition))) {
            final long totalEventsInTimeline = totalEventsInTimeline(timeline, partition);
            final long newShift = shift - (totalEventsInTimeline - offset);
            final Timeline nextTimeline = getTimeline(topic, timeline.getOrder() + 1);
            return moveCursorForward(topic, nextTimeline, partition, 0, newShift);
        } else {
            // Open timeline. Avoid checking for the latest available offset for performance reasons.
            // Might create a cursor that does not exist yet.
            final long finalOffset = offset + shift;
            final String paddedOffset = getOffsetForPosition(timeline, finalOffset);
            return new NakadiCursor(timeline, partition, paddedOffset);
        }
    }

    private String getOffsetForPosition(final Timeline timeline, final long shiftedOffset) {
        return getTopicRepository(timeline).getOffsetForPosition(shiftedOffset);
    }

    private long numberOfEventsBeforeCursor(final NakadiCursor initialCursor) {
        final TopicRepository topicRepository = getTopicRepository(initialCursor.getTimeline());
        return topicRepository.numberOfEventsBeforeCursor(initialCursor);
    }

    private long totalEventsInTimeline(final Timeline timeline, final String partition) {
        return getTopicRepository(timeline).totalEventsInPartition(timeline, partition);
    }

    private long totalEventsInTimeline(final NakadiCursor cursor) {
        final TopicRepository topicRepository = getTopicRepository(cursor.getTimeline());
        return topicRepository.totalEventsInPartition(cursor.getTimeline(), cursor.getPartition());
    }

    private Timeline getTimeline(final String topic, final int order) {
        final List<Timeline> timelines;
        try {
            timelines = timelineService.getActiveTimelinesOrdered(topic);
        } catch (final NakadiException e) {
            throw new RuntimeException(e);
        }
        return timelines.stream()
                .filter(t -> t.getOrder() == order)
                .findFirst()
                .orElseThrow(() -> new InvalidCursorOperation(TIMELINE_NOT_FOUND));
    }

    private TopicRepository getTopicRepository(final Timeline timeline) {
        return timelineService.getTopicRepository(timeline);
    }
}
