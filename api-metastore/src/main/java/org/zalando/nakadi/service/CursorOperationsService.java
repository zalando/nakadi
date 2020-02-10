package org.zalando.nakadi.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.NakadiCursorLag;
import org.zalando.nakadi.domain.PartitionEndStatistics;
import org.zalando.nakadi.domain.PartitionStatistics;
import org.zalando.nakadi.domain.ShiftedNakadiCursor;
import org.zalando.nakadi.domain.storage.Storage;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.exceptions.runtime.InternalNakadiException;
import org.zalando.nakadi.exceptions.runtime.InvalidCursorOperation;
import org.zalando.nakadi.exceptions.runtime.NakadiBaseException;
import org.zalando.nakadi.exceptions.runtime.ServiceTemporarilyUnavailableException;
import org.zalando.nakadi.exceptions.runtime.UnknownStorageTypeException;
import org.zalando.nakadi.repository.kafka.KafkaCursor;
import org.zalando.nakadi.service.timeline.TimelineService;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.zalando.nakadi.exceptions.runtime.InvalidCursorOperation.Reason.CURSORS_WITH_DIFFERENT_PARTITION;
import static org.zalando.nakadi.exceptions.runtime.InvalidCursorOperation.Reason.PARTITION_NOT_FOUND;
import static org.zalando.nakadi.exceptions.runtime.InvalidCursorOperation.Reason.TIMELINE_NOT_FOUND;

@Service
public class CursorOperationsService {
    private final TimelineService timelineService;
    private static final Logger LOG = LoggerFactory.getLogger(CursorOperationsService.class);

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

        int startOrder = Math.min(initialOrder, finalOrder);
        if (startOrder == Timeline.STARTING_ORDER) {
            startOrder += 1;
        }

        for (int order = startOrder; order < Math.max(initialOrder, finalOrder); ++order) {
            final Timeline timeline = getTimeline(initialCursor.getEventType(), order);
            final long eventsTotal = getStorageWorker(timeline)
                    .totalEventsInPartition(timeline, initialCursor.getPartition());
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

                        NakadiCursor newestPosition = newestStats.stream()
                                .filter(item -> item.getPartition().equalsIgnoreCase(c.getPartition()))
                                .map(PartitionEndStatistics::getLast)
                                .findAny().orElseThrow(() -> new InvalidCursorOperation(PARTITION_NOT_FOUND));
                        // trick to avoid -1 position - move cursor to previous timeline while there is no data before
                        // it
                        while (numberOfEventsBeforeCursor(newestPosition) == -1) {
                            final int prevOrder = newestPosition.getTimeline().getOrder() - 1;
                            final Timeline prevTimeline = timelines.stream()
                                    .filter(t -> t.getOrder() == prevOrder)
                                    .findAny().orElse(null);
                            if (null == prevTimeline) {
                                break;
                            }
                            // We moved back, so timeline definitely have latest position set
                            newestPosition = prevTimeline.getLatestPosition()
                                    .toNakadiCursor(prevTimeline, newestPosition.getPartition());
                        }

                        // It is safe to call calculate distance here, cause it will not involve any storage-related
                        // calls (in case of kafka)
                        return new NakadiCursorLag(
                                oldestStat.getFirst(),
                                newestPosition,
                                calculateDistance(c, newestPosition)
                        );
                    }).collect(Collectors.toList());

        } catch (final InternalNakadiException e) {
            throw new NakadiBaseException("error", e);
        }
    }

    private List<PartitionStatistics> getStatsForTimeline(final Timeline timeline)
            throws ServiceTemporarilyUnavailableException {
        return timelineService.getTopicRepository(timeline).loadTopicStatistics(Collections.singletonList(timeline));
    }

    public List<NakadiCursor> unshiftCursors(final List<ShiftedNakadiCursor> cursors) throws InvalidCursorOperation {
        return cursors.stream().map(this::unshiftCursor).collect(Collectors.toList());
    }

    public NakadiCursor unshiftCursor(final ShiftedNakadiCursor cursor) throws InvalidCursorOperation {
        if (cursor.getShift() < 0) {
            return moveBack(cursor);
        } else if (cursor.getShift() > 0) {
            return moveForward(cursor);
        } else {
            return cursor.getNakadiCursor();
        }
    }

    private NakadiCursor moveForward(final ShiftedNakadiCursor cursor) {
        NakadiCursor currentCursor = cursor.getNakadiCursor();
        long stillToAdd = cursor.getShift();
        while (currentCursor.getTimeline().getLatestPosition() != null) {
            final NakadiCursor timelineLastPosition = currentCursor.getTimeline().getLatestPosition()
                    .toNakadiCursor(currentCursor.getTimeline(), currentCursor.getPartition());
            final long distance = calculateDistance(currentCursor, timelineLastPosition);
            if (stillToAdd > distance) {
                stillToAdd -= distance;
                final Timeline nextTimeline = getTimeline(
                        currentCursor.getEventType(), currentCursor.getTimeline().getOrder() + 1);

                currentCursor = NakadiCursor.of(
                        nextTimeline,
                        currentCursor.getPartition(),
                        StaticStorageWorkerFactory.get(nextTimeline).getBeforeFirstOffset());
            } else {
                break;
            }
        }
        if (stillToAdd > 0) {
            return currentCursor.shiftWithinTimeline(stillToAdd);
        }
        return currentCursor;
    }

    private NakadiCursor moveBack(final ShiftedNakadiCursor cursor) {
        NakadiCursor currentCursor = cursor.getNakadiCursor();
        long toMoveBack = -cursor.getShift();
        while (toMoveBack > 0) {
            final long totalBefore = numberOfEventsBeforeCursor(currentCursor);
            if (totalBefore < toMoveBack) {
                toMoveBack -= totalBefore + 1; // +1 is because end is inclusive

                // Next case is a special one. User must have ability to move to the begin (actually - position before
                // begin event that is not within limits)
                if (toMoveBack == 0) {
                    toMoveBack += totalBefore + 1;
                    break;
                }

                final Timeline prevTimeline = getTimeline(
                        currentCursor.getEventType(),
                        currentCursor.getTimeline().getOrder() - 1);
                // When moving back latest position is always defined
                currentCursor = prevTimeline.getLatestPosition()
                        .toNakadiCursor(prevTimeline, currentCursor.getPartition());
            } else {
                break;
            }
        }
        if (toMoveBack != 0) {
            currentCursor = currentCursor.shiftWithinTimeline(-toMoveBack);
        }
        return currentCursor;
    }

    private long numberOfEventsBeforeCursor(final NakadiCursor initialCursor) {
        final Storage.Type storageType = initialCursor.getTimeline().getStorage().getType();
        switch (storageType) {
            case KAFKA:
                return KafkaCursor.toKafkaOffset(initialCursor.getOffset());
            default:
                throw new UnknownStorageTypeException("Unknown storage type: " + storageType.toString());
        }
    }

    private Timeline getTimeline(final String eventTypeName, final int order) {
        final List<Timeline> timelines;
        try {
            timelines = timelineService.getAllTimelinesOrdered(eventTypeName);
        } catch (final InternalNakadiException e) {
            throw new RuntimeException(e);
        }
        return timelines.stream()
                .filter(t -> t.getOrder() == order)
                .findFirst()
                .orElseThrow(() -> new InvalidCursorOperation(TIMELINE_NOT_FOUND));
    }

    private static StaticStorageWorkerFactory.StaticStorageWorker getStorageWorker(final Timeline timeline) {
        return StaticStorageWorkerFactory.get(timeline);
    }

}
