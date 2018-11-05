package org.zalando.nakadi.service.converter;

import com.google.common.annotations.VisibleForTesting;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.exceptions.runtime.InternalNakadiException;
import org.zalando.nakadi.exceptions.runtime.InvalidCursorException;
import org.zalando.nakadi.exceptions.runtime.NoSuchEventTypeException;
import org.zalando.nakadi.exceptions.runtime.ServiceTemporarilyUnavailableException;
import org.zalando.nakadi.repository.db.EventTypeCache;
import org.zalando.nakadi.service.CursorConverter;
import org.zalando.nakadi.service.timeline.TimelineService;
import org.zalando.nakadi.view.Cursor;
import org.zalando.nakadi.view.SubscriptionCursor;
import org.zalando.nakadi.view.SubscriptionCursorWithoutToken;

import java.util.Collection;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

@Service
public class CursorConverterImpl implements CursorConverter {

    private final Map<Version, VersionedConverter> converters = new EnumMap<>(Version.class);

    @Autowired
    public CursorConverterImpl(final EventTypeCache eventTypeCache, final TimelineService timelineService) {
        registerConverter(new VersionOneConverter(eventTypeCache));
        registerConverter(new VersionZeroConverter(timelineService));
    }

    private void registerConverter(final VersionedConverter converter) {
        this.converters.put(converter.getVersion(), converter);
    }

    @Override
    public NakadiCursor convert(final SubscriptionCursorWithoutToken cursor)
            throws InternalNakadiException, NoSuchEventTypeException, ServiceTemporarilyUnavailableException,
            InvalidCursorException {
        return convert(cursor.getEventType(), cursor);
    }

    @Override
    public List<NakadiCursor> convert(final Collection<SubscriptionCursorWithoutToken> cursors)
            throws InternalNakadiException, NoSuchEventTypeException, ServiceTemporarilyUnavailableException,
            InvalidCursorException {
        final LinkedHashMap<SubscriptionCursorWithoutToken, AtomicReference<NakadiCursor>> orderingMap =
                new LinkedHashMap<>();
        cursors.forEach(item -> orderingMap.put(item, new AtomicReference<>(null)));

        final Map<Version, List<SubscriptionCursorWithoutToken>> mappedByVersions = cursors.stream()
                .collect(Collectors.groupingBy(c -> guessVersion(c.getOffset())));
        for (final Map.Entry<Version, List<SubscriptionCursorWithoutToken>> entry : mappedByVersions.entrySet()) {
            final List<NakadiCursor> result = converters.get(entry.getKey()).convertBatched(entry.getValue());
            IntStream.range(0, entry.getValue().size())
                    .forEach(idx -> orderingMap.get(entry.getValue().get(idx)).set(result.get(idx)));
        }
        return orderingMap.values().stream().map(AtomicReference::get).collect(Collectors.toList());
    }

    @Override
    public NakadiCursor convert(final String eventTypeStr, final Cursor cursor)
            throws InternalNakadiException, NoSuchEventTypeException, InvalidCursorException,
            ServiceTemporarilyUnavailableException {
        return converters.get(guessVersion(cursor.getOffset())).convert(eventTypeStr, cursor);
    }

    /**
     * Method tries to get version of cursor. If version can not be restored, than {@link Version#ZERO}
     * will be returned
     *
     * @param offset Offset to guess version from
     * @return Version of offset.
     */
    @VisibleForTesting
    static Version guessVersion(final String offset) {
        if (offset.length() < (CursorConverter.VERSION_LENGTH)) {
            return CursorConverter.Version.ZERO;
        }
        final String versionStr = offset.substring(0, CursorConverter.VERSION_LENGTH);
        final Optional<Version> version =
                Stream.of(CursorConverter.Version.values())
                        .filter(v -> v.code.equals(versionStr))
                        .findAny();
        return version.orElse(CursorConverter.Version.ZERO);
    }

    public Cursor convert(final NakadiCursor nakadiCursor) {
        return new Cursor(
                nakadiCursor.getPartition(),
                converters.get(CursorConverter.Version.ONE).formatOffset(nakadiCursor));
    }

    public SubscriptionCursor convert(final NakadiCursor position, final String token) {
        final Cursor oldCursor = convert(position);
        return new SubscriptionCursor(
                oldCursor.getPartition(), oldCursor.getOffset(), position.getEventType(), token);
    }

    @Override
    public SubscriptionCursorWithoutToken convertToNoToken(final NakadiCursor position) {
        final Cursor oldCursor = convert(position);
        return new SubscriptionCursorWithoutToken(
                position.getEventType(), oldCursor.getPartition(), oldCursor.getOffset());
    }
}
