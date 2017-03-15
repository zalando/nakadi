package org.zalando.nakadi.service;

import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.NakadiCursorDistanceQuery;
import org.zalando.nakadi.domain.NakadiCursorDistanceResult;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.exceptions.NakadiException;
import org.zalando.nakadi.exceptions.runtime.InvalidCursorDistanceQuery;
import org.zalando.nakadi.service.timeline.TimelineService;

import java.util.List;
import java.util.stream.Collectors;

import static org.zalando.nakadi.exceptions.runtime.InvalidCursorDistanceQuery.Reason.CURSORS_WITH_DIFFERENT_PARTITION;
import static org.zalando.nakadi.exceptions.runtime.InvalidCursorDistanceQuery.Reason.INVERTED_OFFSET_ORDER;
import static org.zalando.nakadi.exceptions.runtime.InvalidCursorDistanceQuery.Reason.INVERTED_TIMELINE_ORDER;
import static org.zalando.nakadi.exceptions.runtime.InvalidCursorDistanceQuery.Reason.PARTITION_NOT_FOUND;
import static org.zalando.nakadi.exceptions.runtime.InvalidCursorDistanceQuery.Reason.TIMELINE_NOT_FOUND;

public class CursorOperationsService {
    private final TimelineService timelineService;

    public CursorOperationsService(final TimelineService timelineService) {
        this.timelineService = timelineService;
    }

    public List<NakadiCursorDistanceResult> calculateDistance(final List<NakadiCursorDistanceQuery> cursors)
        throws InvalidCursorDistanceQuery {
        return cursors.stream().map(this::calculateDistance).collect(Collectors.toList());
    }

    public NakadiCursorDistanceResult calculateDistance(final NakadiCursorDistanceQuery query) {
        final NakadiCursor initialCursor = query.getInitialCursor();
        final NakadiCursor finalCursor = query.getFinalCursor();

        if (!initialCursor.getPartition().equals(finalCursor.getPartition())) {
            throw new InvalidCursorDistanceQuery(CURSORS_WITH_DIFFERENT_PARTITION);
        }

        if (initialCursor.getTimeline().getOrder().equals(finalCursor.getTimeline().getOrder())) {
            final long distance = getDistanceSameTimeline(query, initialCursor.getOffset(), finalCursor.getOffset());
            return new NakadiCursorDistanceResult(query, distance);
        } else if (initialCursor.getTimeline().getOrder() > finalCursor.getTimeline().getOrder()) {
            throw new InvalidCursorDistanceQuery(INVERTED_TIMELINE_ORDER);
        } else {
            final long distance = getDistanceDifferentTimelines(query, initialCursor, finalCursor);
            return new NakadiCursorDistanceResult(query, distance);
        }
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
                throw new InvalidCursorDistanceQuery(PARTITION_NOT_FOUND);
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
                .orElseThrow(() -> new InvalidCursorDistanceQuery(TIMELINE_NOT_FOUND));
    }

    private long getDistanceSameTimeline(final NakadiCursorDistanceQuery query, final String initialOffset,
                                         final String finalOffset) {
        final long distance = parse(finalOffset) - parse(initialOffset);
        if (distance < 0) {
            throw new InvalidCursorDistanceQuery(INVERTED_OFFSET_ORDER);
        }
        return distance;
    }

    private long parse(final String paddedOffset) {
        return Long.parseLong(paddedOffset, 10);
    }
}
