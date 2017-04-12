package org.zalando.nakadi.service.converter;

import com.google.common.annotations.VisibleForTesting;
import java.util.EnumMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.zalando.nakadi.domain.CursorError;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.exceptions.InternalNakadiException;
import org.zalando.nakadi.exceptions.InvalidCursorException;
import org.zalando.nakadi.exceptions.NoSuchEventTypeException;
import org.zalando.nakadi.exceptions.ServiceUnavailableException;
import org.zalando.nakadi.repository.db.EventTypeCache;
import org.zalando.nakadi.service.CursorConverter;
import org.zalando.nakadi.service.timeline.TimelineService;
import org.zalando.nakadi.view.Cursor;
import org.zalando.nakadi.view.SubscriptionCursor;
import org.zalando.nakadi.view.SubscriptionCursorWithoutToken;

@Service
public class CursorConverterImpl implements CursorConverter {

    private final Map<NakadiCursor.Version, VersionedConverter> converters = new EnumMap<>(NakadiCursor.Version.class);

    @Autowired
    public CursorConverterImpl(final EventTypeCache eventTypeCache, final TimelineService timelineService) {
        registerConverter(new VersionOneConverter(eventTypeCache, timelineService));
        registerConverter(new VersionZeroConverter(eventTypeCache, timelineService));
    }

    private void registerConverter(final VersionedConverter converter) {
        this.converters.put(converter.getVersion(), converter);
    }

    @Override
    public NakadiCursor convert(final SubscriptionCursorWithoutToken cursor)
            throws InternalNakadiException, NoSuchEventTypeException, ServiceUnavailableException,
            InvalidCursorException {
        return convert(cursor.getEventType(), cursor);
    }

    @Override
    public NakadiCursor convert(final String eventTypeStr, final Cursor cursor)
            throws InternalNakadiException, NoSuchEventTypeException, InvalidCursorException,
            ServiceUnavailableException {
        if (null == cursor.getPartition()) {
            throw new InvalidCursorException(CursorError.NULL_PARTITION, cursor);
        } else if (null == cursor.getOffset()) {
            throw new InvalidCursorException(CursorError.NULL_OFFSET, cursor);
        }

        return converters.get(guessVersion(cursor.getOffset())).convert(eventTypeStr, cursor);
    }

    /**
     * Method tries to get version of cursor. If version can not be restored, than {@link NakadiCursor.Version#ZERO}
     * will be returned
     *
     * @param offset Offset to guess version from
     * @return Version of offset.
     */
    @VisibleForTesting
    static NakadiCursor.Version guessVersion(final String offset) {
        if (offset.length() < (NakadiCursor.VERSION_LENGTH)) {
            return NakadiCursor.Version.ZERO;
        }
        final String versionStr = offset.substring(0, NakadiCursor.VERSION_LENGTH);
        final Optional<NakadiCursor.Version> version =
                Stream.of(NakadiCursor.Version.values())
                        .filter(v -> v.code.equals(versionStr))
                        .findAny();
        return version.orElse(NakadiCursor.Version.ZERO);
    }

    public Cursor convert(final NakadiCursor nakadiCursor) {
        final NakadiCursor.Version version = nakadiCursor.getTimeline().isFake() ? NakadiCursor.Version.ZERO
                : NakadiCursor.Version.ONE;
        return new Cursor(
                nakadiCursor.getPartition(),
                converters.get(version).formatOffset(nakadiCursor));
    }

    public SubscriptionCursor convert(final NakadiCursor position, final String token) {
        final Cursor oldCursor = convert(position);
        return new SubscriptionCursor(
                oldCursor.getPartition(), oldCursor.getOffset(), position.getEventType(), token);
    }
}
