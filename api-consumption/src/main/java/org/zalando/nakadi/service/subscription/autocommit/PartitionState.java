package org.zalando.nakadi.service.subscription.autocommit;

import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.service.CursorOperationsService;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/**
 * There are several assumptions on behavior of this class.
 * 1. Offsets to skip are only added to the end of skipped events queue.
 * 2. Commits are added at any place, and on every commit we are removing queue of intervals to skip that are
 * completely before committed offset.
 * Thus, if we are about to do autocommit, we should check first interval only.
 * 3. Committed offsets are only increasing (this is guaranteed by other part of the code)
 */
public class PartitionState {
    private final CursorOperationsService cursorOperationsService;
    private final List<CursorInterval> skippedIntervals = new ArrayList<>();
    private NakadiCursor committed;

    public PartitionState(final CursorOperationsService cursorOperationsService, final NakadiCursor committed) {
        this.cursorOperationsService = cursorOperationsService;
        this.committed = committed;
    }

    public void addSkippedEvent(final NakadiCursor cursor) {
        if (skippedIntervals.isEmpty()) {
            skippedIntervals.add(new CursorInterval(cursor));
        } else {
            // Cursors are increasing only, therefore we need to check only last one
            final CursorInterval lastInterval = skippedIntervals.get(skippedIntervals.size() - 1);
            if (cursorOperationsService.calculateDistance(lastInterval.getEnd(), cursor) == 1) {
                // We are just increasing interval
                lastInterval.setEnd(cursor);
            } else {
                // have to create another interval
                skippedIntervals.add(new CursorInterval(cursor));
            }
        }
    }

    public void onCommit(final NakadiCursor cursor) {
        this.committed = cursor;
        // remove all the intervals that are fully before committed cursor
        while (!skippedIntervals.isEmpty() &&
                cursorOperationsService.calculateDistance(skippedIntervals.get(0).getEnd(), committed) >= 0) {
            skippedIntervals.remove(0);
        }
    }

    @Nullable
    public NakadiCursor getAutoCommitSuggestion() {
        if (skippedIntervals.isEmpty()) {
            return null;
        }
        final CursorInterval firstToSkip = skippedIntervals.get(0);

        final long distance = cursorOperationsService.calculateDistance(firstToSkip.getBegin(), committed);
        if (distance >= -1) {
            // -1 means that committed is right before first to skip
            return firstToSkip.getEnd();
        }
        return null;
    }
}
