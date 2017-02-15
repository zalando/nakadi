package org.zalando.nakadi.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.exceptions.InvalidCursorException;
import org.zalando.nakadi.exceptions.NakadiRuntimeException;
import org.zalando.nakadi.repository.TopicRepository;
import org.zalando.nakadi.view.SubscriptionCursor;

import java.util.List;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;

import static org.zalando.nakadi.view.Cursor.BEFORE_OLDEST_OFFSET;

public class CursorCompactor {

    private final TopicRepository topicRepository;
    private final BinaryOperator<SubscriptionCursor> getMoreRecentOffset;

    @Autowired
    public CursorCompactor(final TopicRepository topicRepository) {
        this.topicRepository = topicRepository;
        this.getMoreRecentOffset = (c1, c2) -> {
            final NakadiCursor nc1 = toNakadiCursor(c1);
            final NakadiCursor nc2 = toNakadiCursor(c2);
            try {
                return this.topicRepository.compareOffsets(nc1, nc2) > 0 ? c1 : c2;
            } catch (final InvalidCursorException e) {
                throw new NakadiRuntimeException(e);
            }
        };
    }

    public List<SubscriptionCursor> compactCursors(final List<SubscriptionCursor> cursors)
            throws InvalidCursorException {
        try {
            return cursors.stream()
                    .map(SubscriptionCursor::getPartition)
                    .distinct()
                    .map(p -> cursors.stream()
                            .filter(c -> c.getPartition().equals(p))
                            .reduce(getMoreRecentOffset)
                            .get())
                    .collect(Collectors.toList());
        } catch (final NakadiRuntimeException e) {
            throw (InvalidCursorException) e.getException();
        }
    }

    // todo: this should be removed after Timelines are implemented
    private static NakadiCursor toNakadiCursor(final SubscriptionCursor cursor) {
        return new NakadiCursor(
                cursor.getEventType(),
                cursor.getPartition(),
                BEFORE_OLDEST_OFFSET.equalsIgnoreCase(cursor.getOffset()) ? "-1" : cursor.getOffset());
    }

}
