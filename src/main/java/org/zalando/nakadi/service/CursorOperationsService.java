package org.zalando.nakadi.service;

import org.apache.commons.lang3.StringUtils;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.NakadiCursorDistanceQuery;
import org.zalando.nakadi.domain.NakadiCursorDistanceResult;
import org.zalando.nakadi.domain.ShiftedNakadiCursor;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.exceptions.NakadiException;
import org.zalando.nakadi.exceptions.runtime.InvalidCursorOperation;
import org.zalando.nakadi.service.timeline.TimelineService;

import java.util.List;
import java.util.stream.Collectors;

import static org.zalando.nakadi.exceptions.runtime.InvalidCursorOperation.Reason.CURSORS_WITH_DIFFERENT_PARTITION;
import static org.zalando.nakadi.exceptions.runtime.InvalidCursorOperation.Reason.INVERTED_OFFSET_ORDER;
import static org.zalando.nakadi.exceptions.runtime.InvalidCursorOperation.Reason.INVERTED_TIMELINE_ORDER;
import static org.zalando.nakadi.exceptions.runtime.InvalidCursorOperation.Reason.PARTITION_NOT_FOUND;
import static org.zalando.nakadi.exceptions.runtime.InvalidCursorOperation.Reason.TIMELINE_NOT_FOUND;

public class CursorOperationsService {
    public static final int CURSOR_OFFSET_LENGTH = 18;

    private final TimelineService timelineService;

    public CursorOperationsService(final TimelineService timelineService) {
        this.timelineService = timelineService;
    }

    public List<NakadiCursorDistanceResult> calculateDistance(final List<NakadiCursorDistanceQuery> cursors)
        throws InvalidCursorOperation {
        return cursors.stream().map(this::calculateDistance).collect(Collectors.toList());
    }

    public NakadiCursorDistanceResult calculateDistance(final NakadiCursorDistanceQuery query) {
        final NakadiCursor initialCursor = query.getInitialCursor();
        final NakadiCursor finalCursor = query.getFinalCursor();

        if (!initialCursor.getPartition().equals(finalCursor.getPartition())) {
            throw new InvalidCursorOperation(CURSORS_WITH_DIFFERENT_PARTITION);
        }

        if (initialCursor.getTimeline().getOrder().equals(finalCursor.getTimeline().getOrder())) {
            final long distance = getDistanceSameTimeline(query, initialCursor.getOffset(), finalCursor.getOffset());
            return new NakadiCursorDistanceResult(query, distance);
        } else if (initialCursor.getTimeline().getOrder() > finalCursor.getTimeline().getOrder()) {
            throw new InvalidCursorOperation(INVERTED_TIMELINE_ORDER);
        } else {
            final long distance = getDistanceDifferentTimelines(query, initialCursor, finalCursor);
            return new NakadiCursorDistanceResult(query, distance);
        }
    }

    public NakadiCursor unshiftCursor(final ShiftedNakadiCursor cursor) throws InvalidCursorOperation {
        if (cursor.getShift() < 0) {
            return moveCursorBackwards(cursor.getTopic(), cursor.getTimeline(), cursor.getPartition(),
                    parse(cursor.getOffset()), cursor.getShift());
        } else {
            return moveCursorForward(cursor.getTopic(), cursor.getTimeline(), cursor.getPartition(),
                    parse(cursor.getOffset()), cursor.getShift());
        }
    }

    private NakadiCursor moveCursorBackwards(final String topic, final Timeline timeline, final String partition,
                                             final long offset, final long shift) {
        final long shiftedOffset = offset + shift;
        if (shiftedOffset >= 0) { // move left in the same timeline
            final String paddedOffset = getPaddedOffset(shiftedOffset);
            return new NakadiCursor(timeline, partition, paddedOffset);
        } else { // move to previous timeline
            final Timeline previousTimeline = getTimeline(topic, timeline.getOrder() - 1);
            final long totalEventsInTimeline = totalEventsInTimeline(previousTimeline, partition);
            return moveCursorBackwards(topic, previousTimeline, partition, totalEventsInTimeline, shift + offset);
        }
    }

    private NakadiCursor moveCursorForward(final String topic, final Timeline timeline, final String partition,
                                    final long offset, final long shift) {
        if (timeline.isClosed()) {
            if (offset + shift < totalEventsInTimeline(timeline, partition)) { // move right in the same timeline
                final long finalOffset = offset + shift;
                final String paddedOffset = getPaddedOffset(finalOffset);
                return new NakadiCursor(timeline, partition, paddedOffset);
            } else { // move right, next timeline
                final Timeline nextTimeline = getTimeline(topic, timeline.getOrder() + 1);
                final long totalEventsInTimeline = totalEventsInTimeline(nextTimeline, partition);
                final long newShift = shift - (totalEventsInTimeline - offset);
                return moveCursorForward(topic, nextTimeline, partition, 0, newShift);
            }
        } else { // Open timeline. Avoid checking for the latest available offset for performance reasons
            final long finalOffset = offset + shift;
            final String paddedOffset = getPaddedOffset(finalOffset);
            return new NakadiCursor(timeline, partition, paddedOffset);
        }
    }

    private String getPaddedOffset(final long finalOffset) {
        return StringUtils.leftPad(String.valueOf(finalOffset), CURSOR_OFFSET_LENGTH, '0');
    }

    private long getDistanceDifferentTimelines(final NakadiCursorDistanceQuery query, final NakadiCursor initialCursor,
                                               final NakadiCursor finalCursor) {
        long distance = 0;

        // get distance from initialCursor to the end of the timeline
        distance += totalEventsInTimeline(initialCursor) - parse(initialCursor.getOffset());

        // get all intermediary timelines sizes
        final String partitionString = initialCursor.getPartition();
        for (int order = initialCursor.getTimeline().getOrder() + 1;
             order < finalCursor.getTimeline().getOrder();
             order++) {
            final Timeline t = getTimeline(initialCursor.getTopic(), order);
            distance += totalEventsInTimeline(t, partitionString);
        }

        // do not check if the offset exists because there is no reason to do it.
        distance += parse(finalCursor.getOffset());

        return distance;
    }

    private long totalEventsInTimeline(final Timeline timeline, final String partitionString) {
        final Timeline.StoragePosition positions = timeline.getLatestPosition();
        if (positions instanceof Timeline.KafkaStoragePosition) {
            final int partition = Integer.valueOf(partitionString);
            final Timeline.KafkaStoragePosition kafkaPositions = (Timeline.KafkaStoragePosition) positions;
            final List<Long> offsets = kafkaPositions.getOffsets();
            if (offsets.size() - 1 < partition) {
                throw new InvalidCursorOperation(PARTITION_NOT_FOUND);
            } else {
                return offsets.get(partition);
            }
        } else {
            throw new RuntimeException("operation not implemented for storage " + positions.getClass());
        }
    }

    private long totalEventsInTimeline(final NakadiCursor cursor) {
        return totalEventsInTimeline(cursor.getTimeline(), cursor.getPartition());
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

    private long getDistanceSameTimeline(final NakadiCursorDistanceQuery query, final String initialOffset,
                                         final String finalOffset) {
        final long distance = parse(finalOffset) - parse(initialOffset);
        if (distance < 0) {
            throw new InvalidCursorOperation(INVERTED_OFFSET_ORDER);
        }
        return distance;
    }

    private long parse(final String paddedOffset) {
        return Long.parseLong(paddedOffset, 10);
    }
}
