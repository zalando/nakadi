package org.zalando.nakadi.service.converter;

import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.exceptions.runtime.InternalNakadiException;
import org.zalando.nakadi.exceptions.runtime.InvalidCursorException;
import org.zalando.nakadi.exceptions.runtime.NoSuchEventTypeException;
import org.zalando.nakadi.exceptions.runtime.ServiceTemporarilyUnavailableException;
import org.zalando.nakadi.service.CursorConverter;
import org.zalando.nakadi.view.Cursor;
import org.zalando.nakadi.view.SubscriptionCursorWithoutToken;

import java.util.List;

interface VersionedConverter {

    CursorConverter.Version getVersion();

    List<NakadiCursor> convertBatched(List<SubscriptionCursorWithoutToken> cursors) throws
            InvalidCursorException, InternalNakadiException, NoSuchEventTypeException,
            ServiceTemporarilyUnavailableException;

    /**
     * Converts cursor to nakadi cursor having in mind that preliminary checks for format were performed.
     *
     * @param eventTypeStr Name of event type
     * @param cursor       cursor to convert.
     * @return NakadiCursor instance.
     */
    NakadiCursor convert(String eventTypeStr, Cursor cursor) throws
            InternalNakadiException, NoSuchEventTypeException, ServiceTemporarilyUnavailableException,
            InvalidCursorException;

    String formatOffset(NakadiCursor nakadiCursor);
}
