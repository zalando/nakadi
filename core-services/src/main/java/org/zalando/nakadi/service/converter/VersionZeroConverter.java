package org.zalando.nakadi.service.converter;

import org.apache.commons.lang3.StringUtils;
import org.zalando.nakadi.domain.CursorError;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.PartitionStatistics;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.exceptions.runtime.InternalNakadiException;
import org.zalando.nakadi.exceptions.runtime.InvalidCursorException;
import org.zalando.nakadi.exceptions.runtime.NoSuchEventTypeException;
import org.zalando.nakadi.exceptions.runtime.ServiceTemporarilyUnavailableException;
import org.zalando.nakadi.repository.TopicRepository;
import org.zalando.nakadi.service.CursorConverter;
import org.zalando.nakadi.service.timeline.TimelineService;
import org.zalando.nakadi.util.CursorConversionUtils;
import org.zalando.nakadi.view.Cursor;
import org.zalando.nakadi.view.SubscriptionCursorWithoutToken;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class VersionZeroConverter implements VersionedConverter {
    public static final int VERSION_ZERO_MIN_OFFSET_LENGTH = 18;
    private final TimelineService timelineService;

    VersionZeroConverter(final TimelineService timelineService) {
        this.timelineService = timelineService;
    }

    @Override
    public CursorConverter.Version getVersion() {
        return CursorConverter.Version.ZERO;
    }

    public List<NakadiCursor> convertBatched(final List<SubscriptionCursorWithoutToken> cursors) throws
            InvalidCursorException, InternalNakadiException, NoSuchEventTypeException,
            ServiceTemporarilyUnavailableException {
        final NakadiCursor[] result = new NakadiCursor[cursors.size()];
        for (int idx = 0; idx < cursors.size(); ++idx) {
            final SubscriptionCursorWithoutToken cursor = cursors.get(idx);
            if (Cursor.BEFORE_OLDEST_OFFSET.equalsIgnoreCase(cursor.getOffset())) {
                // Preform begin checks afterwards to optimize calls
                continue;
            }
            if (!CursorConversionUtils.NUMBERS_ONLY_PATTERN.matcher(cursor.getOffset()).matches()) {
                throw new InvalidCursorException(CursorError.INVALID_OFFSET, cursor, cursor.getEventType());
            }
        }
        // now it is time for massive convert.
        final LinkedHashMap<SubscriptionCursorWithoutToken, NakadiCursor> beginsToConvert = new LinkedHashMap<>();
        final Map<SubscriptionCursorWithoutToken, Timeline> cursorTimelines = new HashMap<>();
        final Map<TopicRepository, List<SubscriptionCursorWithoutToken>> repos = new HashMap<>();
        for (int i = 0; i < result.length; ++i) {
            if (null == result[i]) { // cursor requires database hit
                final SubscriptionCursorWithoutToken cursor = cursors.get(i);
                final Timeline timeline = timelineService.getActiveTimelinesOrdered(cursor.getEventType()).get(0);
                final TopicRepository topicRepo = timelineService.getTopicRepository(timeline);
                beginsToConvert.put(cursor, null);
                cursorTimelines.put(cursor, timeline);
                repos.computeIfAbsent(topicRepo, k -> new ArrayList<>()).add(cursor);
            }
        }
        // Now, when everything prepared, one can call in parallel for conversion.

        for (final Map.Entry<TopicRepository, List<SubscriptionCursorWithoutToken>> entry : repos.entrySet()) {
            final List<Optional<PartitionStatistics>> stats = entry.getKey().loadPartitionStatistics(
                    entry.getValue().stream()
                            .map(scwt -> new TopicRepository.TimelinePartition(
                                    cursorTimelines.get(scwt),
                                    scwt.getPartition()
                            ))
                            .collect(Collectors.toList())
            );
            for (int idx = 0; idx < entry.getValue().size(); ++idx) {
                // Reinsert doesn't change the order
                final SubscriptionCursorWithoutToken val = entry.getValue().get(idx);
                beginsToConvert.put(
                        val,
                        stats.get(idx)
                                .orElseThrow(() -> new InvalidCursorException(
                                        CursorError.PARTITION_NOT_FOUND, val.getEventType()))
                                .getBeforeFirst());
            }
        }
        final Iterator<NakadiCursor> missingBegins = beginsToConvert.values().iterator();

        return Stream.of(result)
                .map(it -> null == it ? missingBegins.next() : it)
                .collect(Collectors.toList());
    }

    @Override
    public NakadiCursor convert(final String eventTypeStr, final Cursor cursor) throws
            InternalNakadiException, NoSuchEventTypeException, ServiceTemporarilyUnavailableException,
            InvalidCursorException {
        final String offset = cursor.getOffset();
        if (Cursor.BEFORE_OLDEST_OFFSET.equalsIgnoreCase(offset)) {
            final Timeline timeline = timelineService.getActiveTimelinesOrdered(eventTypeStr).get(0);
            return timelineService.getTopicRepository(timeline)
                    .loadPartitionStatistics(timeline, cursor.getPartition())
                    .orElseThrow(() -> new InvalidCursorException(CursorError.PARTITION_NOT_FOUND, eventTypeStr))
                    .getBeforeFirst();
        } else if (!CursorConversionUtils.NUMBERS_ONLY_PATTERN.matcher(offset).matches()) {
            throw new InvalidCursorException(CursorError.INVALID_OFFSET, cursor, eventTypeStr);
        }

        final Timeline timeline = timelineService.getAllTimelinesOrdered(eventTypeStr).get(0);
        if (offset.startsWith("-")) {
            return NakadiCursor.of(timeline, cursor.getPartition(), cursor.getOffset());
        } else {
            return NakadiCursor.of(
                    timeline,
                    cursor.getPartition(),
                    StringUtils.leftPad(cursor.getOffset(), VERSION_ZERO_MIN_OFFSET_LENGTH, '0'));
        }
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
