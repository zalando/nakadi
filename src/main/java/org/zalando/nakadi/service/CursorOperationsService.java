package org.zalando.nakadi.service;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.NakadiCursorLag;
import org.zalando.nakadi.domain.PartitionStatistics;
import org.zalando.nakadi.domain.ShiftedNakadiCursor;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.exceptions.InternalNakadiException;
import org.zalando.nakadi.exceptions.NakadiException;
import org.zalando.nakadi.exceptions.NoSuchEventTypeException;
import org.zalando.nakadi.exceptions.NotFoundException;
import org.zalando.nakadi.exceptions.ServiceUnavailableException;
import org.zalando.nakadi.exceptions.runtime.InvalidCursorOperation;
import static org.zalando.nakadi.exceptions.runtime.InvalidCursorOperation.Reason.CURSORS_WITH_DIFFERENT_PARTITION;
import static org.zalando.nakadi.exceptions.runtime.InvalidCursorOperation.Reason.PARTITION_NOT_FOUND;
import static org.zalando.nakadi.exceptions.runtime.InvalidCursorOperation.Reason.TIMELINE_NOT_FOUND;
import org.zalando.nakadi.exceptions.runtime.MyNakadiRuntimeException1;
import org.zalando.nakadi.repository.TopicRepository;
import org.zalando.nakadi.service.timeline.TimelineService;

@Service
public class CursorOperationsService {
    private final TimelineService timelineService;

    @Autowired
    public CursorOperationsService(final TimelineService timelineService) {
        this.timelineService = timelineService;
    }

    public long calculateDistance(final NakadiCursor initialCursor, final NakadiCursor finalCursor)
            throws InvalidCursorOperation {
        // Validate query
        if (!initialCursor.getPartition().equals(finalCursor.getPartition())) {
            throw new InvalidCursorOperation(CURSORS_WITH_DIFFERENT_PARTITION);
        }

        long result = numberOfEventsBeforeCursor(finalCursor) - numberOfEventsBeforeCursor(initialCursor);
        final int initialOrder = initialCursor.getTimeline().getOrder();
        final int finalOrder = finalCursor.getTimeline().getOrder();

        // In case when fake timeline is still there, one should count it only once.
        // eg. in case of offset 1234 it should be treated as 0001-0001-000...1234
        int startOrder = Math.min(initialOrder, finalOrder);
        if (startOrder == Timeline.STARTING_ORDER) {
            startOrder += 1;
        }

        for (int order = startOrder; order < Math.max(initialOrder, finalOrder); ++order) {
            final Timeline timeline = getTimeline(initialCursor.getEventType(), order);
            final long eventsTotal = timelineService.getTopicRepository(timeline).totalEventsInPartition(
                    timeline, initialCursor.getPartition());
            result += (finalOrder > initialOrder) ? eventsTotal : -eventsTotal;
        }
        return result;
    }


    public List<NakadiCursorLag> cursorsLag(final String eventTypeName, final List<NakadiCursor> cursors)
            throws InvalidCursorOperation {
        try {
            final List<Timeline> timelines = timelineService.getActiveTimelinesOrdered(eventTypeName);
            // Next 2 calls could be optimized to 1 storage call, instead of possible 2 calls.
            // But it is simpler not to do anything, cause timelines are not switched every day and almost all the time
            // (except retention time after switch) there will be only 1 active timeline, and this option is covered.
            final List<PartitionStatistics> oldestStats = getStatsForTimeline(timelines.get(0));
            final List<PartitionStatistics> newestStats = timelines.size() == 1 ? oldestStats :
                    getStatsForTimeline(timelines.get(timelines.size() - 1));

            return cursors.stream()
                    .map(c -> {
                        final PartitionStatistics oldestStat = oldestStats.stream()
                                .filter(item -> item.getPartition().equalsIgnoreCase(c.getPartition()))
                                .findAny().orElseThrow(() -> new InvalidCursorOperation(PARTITION_NOT_FOUND));

                        final PartitionStatistics newestStat = newestStats.stream()
                                .filter(item -> item.getPartition().equalsIgnoreCase(c.getPartition()))
                                .findAny().orElseThrow(() -> new InvalidCursorOperation(PARTITION_NOT_FOUND));

                        // It is safe to call calculate distance here, cause it will not involve any storage-related
                        // calls (in case of kafka)
                        return new NakadiCursorLag(
                                oldestStat.getFirst(),
                                newestStat.getLast(),
                                calculateDistance(newestStat.getLast(), c)
                        );
                    }).collect(Collectors.toList());

        } catch (final NakadiException e) {
            throw new MyNakadiRuntimeException1("error", e);
        }
    }

    private List<PartitionStatistics> getStatsForTimeline(final Timeline timeline) throws ServiceUnavailableException {
        return timelineService.getTopicRepository(timeline)
                .loadTopicStatistics(Collections.singletonList(timeline));
    }

    public List<NakadiCursor> unshiftCursors(final List<ShiftedNakadiCursor> cursors) throws InvalidCursorOperation {
        return cursors.stream().map(this::unshiftCursor).collect(Collectors.toList());
    }

    public NakadiCursor unshiftCursor(final ShiftedNakadiCursor cursor) throws InvalidCursorOperation {
        if (cursor.getShift() < 0) {
            return moveCursorBackwards(cursor.getEventType(), cursor.getTimeline(), cursor.getPartition(),
                    numberOfEventsBeforeCursor(cursor),
                    cursor.getShift());
        } else if (cursor.getShift() > 0) {
            Timeline processedTimeline = cursor.getTimeline();
            long stillToAdd = cursor.getShift();

            NakadiCursor current = cursor;
            while (processedTimeline.getLatestPosition() != null) {
                final NakadiCursor timelineLastPosition = processedTimeline.getLatestPosition()
                        .toNakadiCursor(processedTimeline, cursor.getPartition());
                final long distance = calculateDistance(current, timelineLastPosition);
                if (stillToAdd > distance) {
                    stillToAdd -= distance;
                    processedTimeline = getTimeline(current.getEventType(), processedTimeline.getOrder() + 1);
                    current = timelineService.getTopicRepository(processedTimeline)
                            .createBeforeBeginCursor(processedTimeline, current.getPartition());
                } else {
                    break;
                }
            }
            if (stillToAdd > 0) {
                return getTopicRepository(current.getTimeline()).shiftWithinTimeline(current, stillToAdd);
            }
        } else {
            return new NakadiCursor(cursor.getTimeline(), cursor.getPartition(), cursor.getOffset());
        }
    }

    private NakadiCursor moveCursorBackwards(final String eventTypeName, final Timeline timeline,
                                             final String partition, final long offset, final long shift) {
        final long shiftedOffset = offset + shift;
        if (shiftedOffset >= 0 ||
                isBegin(eventTypeName, timeline, shiftedOffset)) { // move left in the same timeline
            final String paddedOffset = getOffsetForPosition(timeline, shiftedOffset);
            return new NakadiCursor(timeline, partition, paddedOffset);
        } else { // move to previous timeline
            final Timeline previousTimeline = getTimeline(eventTypeName, timeline.getOrder() - 1);
            final long totalEventsInTimeline = totalEventsInTimeline(previousTimeline, partition);
            return moveCursorBackwards(eventTypeName, previousTimeline, partition, totalEventsInTimeline,
                    shift + offset);
        }
    }

    private boolean isBegin(final String eventTypeName, final Timeline timeline, final long shiftedOffset) {
        try {
            return shiftedOffset == -1 && timeline.getOrder() == timelineService
                    .getActiveTimelinesOrdered(eventTypeName).get(0).getOrder();
        } catch (final InternalNakadiException e) {
            throw new RuntimeException(e);
        } catch (final NoSuchEventTypeException e) {
            throw new NotFoundException("event type not found " + eventTypeName, e);
        }
    }

    private NakadiCursor moveCursorForward(final String eventType, final Timeline timeline, final String partition,
                                           final long offset, final long shift) {
        if (offset + shift < totalEventsInTimeline(timeline, partition)) {
            final long finalOffset = offset + shift;
            final String paddedOffset = getOffsetForPosition(timeline, finalOffset);
            return new NakadiCursor(timeline, partition, paddedOffset);
        } else {
            final long totalEventsInTimeline = totalEventsInTimeline(timeline, partition);
            final long newShift = shift - (totalEventsInTimeline - offset);
            final Timeline nextTimeline = getTimeline(eventType, timeline.getOrder() + 1);
            return moveCursorForward(eventType, nextTimeline, partition, 0, newShift);
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

    private Timeline getTimeline(final String eventTypeName, final int order) {
        final List<Timeline> timelines;
        try {
            timelines = timelineService.getActiveTimelinesOrdered(eventTypeName);
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
