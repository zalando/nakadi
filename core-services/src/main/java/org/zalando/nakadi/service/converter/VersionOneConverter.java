package org.zalando.nakadi.service.converter;

import org.zalando.nakadi.cache.EventTypeCache;
import org.zalando.nakadi.domain.CursorError;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.exceptions.runtime.InternalNakadiException;
import org.zalando.nakadi.exceptions.runtime.InvalidCursorException;
import org.zalando.nakadi.exceptions.runtime.NoSuchEventTypeException;
import org.zalando.nakadi.service.CursorConverter;
import org.zalando.nakadi.service.StaticStorageWorkerFactory;
import org.zalando.nakadi.util.CursorConversionUtils;
import org.zalando.nakadi.view.Cursor;
import org.zalando.nakadi.view.SubscriptionCursorWithoutToken;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

class VersionOneConverter implements VersionedConverter {
    private static final int TIMELINE_ORDER_LENGTH = 4;
    private static final int TIMELINE_ORDER_BASE = 16;

    private final EventTypeCache eventTypeCache;

    VersionOneConverter(final EventTypeCache eventTypeCache) {
        this.eventTypeCache = eventTypeCache;
    }

    @Override
    public CursorConverter.Version getVersion() {
        return CursorConverter.Version.ONE;
    }

    @Override
    public List<NakadiCursor> convertBatched(final List<SubscriptionCursorWithoutToken> cursors) throws
            InvalidCursorException, InternalNakadiException, NoSuchEventTypeException {
        // For version one it doesn't matter - batched or not
        final List<NakadiCursor> result = new ArrayList<>(cursors.size());
        for (final SubscriptionCursorWithoutToken cursor : cursors) {
            result.add(convert(cursor.getEventType(), cursor));
        }
        return result;
    }

    public NakadiCursor convert(final String eventTypeStr, final Cursor cursor)
            throws InternalNakadiException, NoSuchEventTypeException, InvalidCursorException {
        final String[] parts = cursor.getOffset().split("-", 3);
        if (parts.length != 3) {
            throw new InvalidCursorException(CursorError.INVALID_OFFSET, eventTypeStr);
        }
        final String versionStr = parts[0];
        if (versionStr.length() != CursorConverter.VERSION_LENGTH) {
            throw new InvalidCursorException(CursorError.INVALID_OFFSET, eventTypeStr);
        }
        final String orderStr = parts[1];
        if (orderStr.length() != TIMELINE_ORDER_LENGTH) {
            throw new InvalidCursorException(CursorError.INVALID_OFFSET, eventTypeStr);
        }
        final String offsetStr = parts[2];
        if (offsetStr.isEmpty() || !CursorConversionUtils.NUMBERS_ONLY_PATTERN.matcher(offsetStr).matches()) {
            throw new InvalidCursorException(CursorError.INVALID_OFFSET, eventTypeStr);
        }
        final int order;
        try {
            order = Integer.parseInt(orderStr, TIMELINE_ORDER_BASE);
        } catch (final NumberFormatException ex) {
            throw new InvalidCursorException(CursorError.INVALID_OFFSET, eventTypeStr);
        }
        return findCorrectTimelinedCursor(eventTypeStr, order, cursor.getPartition(), offsetStr);
    }

    private NakadiCursor findCorrectTimelinedCursor(
            final String eventType, final int order, final String partition, final String offset)
            throws InternalNakadiException, NoSuchEventTypeException, InvalidCursorException {
        final List<Timeline> timelines = eventTypeCache.getTimelinesOrdered(eventType);
        final Iterator<Timeline> timelineIterator = timelines.iterator();
        Timeline timeline = null;
        while (timelineIterator.hasNext()) {
            final Timeline t = timelineIterator.next();
            if (t.getOrder() == order) {
                timeline = t;
                break;
            }
        }
        if (null == timeline) {
            throw new InvalidCursorException(CursorError.UNAVAILABLE, eventType);
        }
        NakadiCursor cursor = NakadiCursor.of(timeline, partition, offset);
        while (cursor.isLast()) {
            // Will not check this call, because latest offset is not set for last timeline
            timeline = timelineIterator.next();
            cursor = NakadiCursor.of(
                    timeline,
                    partition,
                    StaticStorageWorkerFactory.get(timeline).getBeforeFirstOffset());
        }
        return cursor;
    }

    public String formatOffset(final NakadiCursor nakadiCursor) {
        // Cursor is made of fields.
        // version - 3 symbols
        // order - 4 symbols
        // offset data - everything else
        return String.format(
                "%s-%04x-%s",
                CursorConverter.Version.ONE.code,
                nakadiCursor.getTimeline().getOrder(),
                nakadiCursor.getOffset());
    }
}
