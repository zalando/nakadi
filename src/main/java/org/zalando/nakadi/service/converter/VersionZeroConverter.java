package org.zalando.nakadi.service.converter;

import org.apache.commons.lang3.StringUtils;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.exceptions.InternalNakadiException;
import org.zalando.nakadi.exceptions.InvalidCursorException;
import org.zalando.nakadi.exceptions.NoSuchEventTypeException;
import org.zalando.nakadi.exceptions.ServiceUnavailableException;
import org.zalando.nakadi.repository.db.EventTypeCache;
import org.zalando.nakadi.service.CursorConverter;
import org.zalando.nakadi.service.timeline.TimelineService;
import org.zalando.nakadi.view.Cursor;
import static org.zalando.nakadi.domain.CursorError.PARTITION_NOT_FOUND;

class VersionZeroConverter implements VersionedConverter {
    static final int VERSION_ZERO_MIN_OFFSET_LENGTH = 18;
    private final EventTypeCache eventTypeCache;
    private final TimelineService timelineService;

    VersionZeroConverter(final EventTypeCache eventTypeCache, final TimelineService timelineService) {
        this.eventTypeCache = eventTypeCache;
        this.timelineService = timelineService;
    }

    @Override
    public CursorConverter.Version getVersion() {
        return CursorConverter.Version.ZERO;
    }

    @Override
    public NakadiCursor convert(final String eventTypeStr, final Cursor cursor) throws
            InternalNakadiException, NoSuchEventTypeException, ServiceUnavailableException, InvalidCursorException {
        final EventType eventType = eventTypeCache.getEventType(eventTypeStr);
        final String offset = cursor.getOffset();
        if (Cursor.BEFORE_OLDEST_OFFSET.equalsIgnoreCase(offset)) {
            final Timeline timeline = timelineService.getActiveTimelinesOrdered(eventTypeStr).get(0);
            return timelineService.getTopicRepository(timeline)
                    .loadPartitionStatistics(timeline, cursor.getPartition())
                    .orElseThrow(() -> new InvalidCursorException(PARTITION_NOT_FOUND))
                    .getBeforeFirst();
        }
        return new NakadiCursor(
                timelineService.getFakeTimeline(eventType),
                cursor.getPartition(),
                StringUtils.leftPad(cursor.getOffset(), VERSION_ZERO_MIN_OFFSET_LENGTH, '0'));

    }

    public String formatOffset(final NakadiCursor nakadiCursor) {
        if (nakadiCursor.getOffset().equals("-1")) {
            // TODO: Before old should be calculated differently
            return Cursor.BEFORE_OLDEST_OFFSET;
        } else {
            return StringUtils.leftPad(nakadiCursor.getOffset(), VERSION_ZERO_MIN_OFFSET_LENGTH, '0');
        }
    }


}
