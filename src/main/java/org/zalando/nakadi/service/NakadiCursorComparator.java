package org.zalando.nakadi.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.exceptions.runtime.InternalNakadiException;
import org.zalando.nakadi.exceptions.runtime.NoSuchEventTypeException;
import org.zalando.nakadi.repository.db.EventTypeCache;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

@Component
public class NakadiCursorComparator implements Comparator<NakadiCursor> {

    private final EventTypeCache eventTypeCache;

    @Autowired
    public NakadiCursorComparator(final EventTypeCache eventTypeCache) {
        this.eventTypeCache = eventTypeCache;
    }

    public int compare(final NakadiCursor c1, final NakadiCursor c2) {
        if (!Objects.equals(c1.getEventType(), c2.getEventType())) {
            throw new IllegalArgumentException("Cursors from different event types are not comparable");
        }
        if (c1.getTimeline().getOrder() == c2.getTimeline().getOrder()) {
            return c1.getOffset().compareTo(c2.getOffset());
        }
        if (c1.getTimeline().getOrder() > c2.getTimeline().getOrder()) {
            return -compareOrdered(c2, c1);
        } else {
            return compareOrdered(c1, c2);
        }
    }

    private int compareOrdered(final NakadiCursor c1, final NakadiCursor c2) {
        // Disclaimer: The reason of this method complexity is to avoid objects creation.

        // If c2 moved from -1, than it is definitely greater.
        if (!c2.isInitial()) {
            return -1;
        }

        Iterator<Timeline> timelineIterator = null;

        NakadiCursor first = c1;
        // Handle obsolete timeline information
        if (first.getTimeline().getLatestPosition() == null) {
            timelineIterator = createTimelinesIterator(first.getEventType(), first.getTimeline().getOrder());
            first = NakadiCursor.of(timelineIterator.next(), first.getPartition(), first.getOffset());
        }

        while (first.getTimeline().getOrder() != c2.getTimeline().getOrder()) {
            if (!first.isLast()) {
                return -1;
            }
            if (null == timelineIterator) {
                timelineIterator = createTimelinesIterator(first.getEventType(), first.getTimeline().getOrder() + 1);
            }
            final Timeline nextTimeline = timelineIterator.next();
            final String initialOffset = StaticStorageWorkerFactory.get(nextTimeline).getBeforeFirstOffset();
            first = NakadiCursor.of(nextTimeline, first.getPartition(), initialOffset);
        }
        return first.getOffset().compareTo(c2.getOffset());
    }


    private Iterator<Timeline> createTimelinesIterator(final String eventType, final int order) {
        try {
            final List<Timeline> timelines = eventTypeCache.getTimelinesOrdered(eventType);
            final Iterator<Timeline> result = timelines.iterator();
            if (timelines.get(0).getOrder() == order) {
                return result;
            }
            // I do not want to handle hasNext
            while (result.next().getOrder() != (order - 1)) {
            }
            return result;
        } catch (final NoSuchEventTypeException | InternalNakadiException ex) {
            // The reason for runtime exception is that cursors are constructed before, and probably should be working.
            // Otherwise it makes no sense for this exception.
            throw new IllegalStateException(ex);
        }
    }
}
