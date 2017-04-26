package org.zalando.nakadi.service.converter;

import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.exceptions.InternalNakadiException;
import org.zalando.nakadi.exceptions.InvalidCursorException;
import org.zalando.nakadi.exceptions.NoSuchEventTypeException;
import org.zalando.nakadi.exceptions.ServiceUnavailableException;
import org.zalando.nakadi.service.CursorConverter;
import org.zalando.nakadi.view.Cursor;

interface VersionedConverter {

    CursorConverter.Version getVersion();

    /**
     * Converts cursor to nakadi cursor having in mind that preliminary checks for format were performed.
     *
     * @param eventTypeStr Name of event type
     * @param cursor       cursor to convert.
     * @return NakadiCursor instance.
     */
    NakadiCursor convert(String eventTypeStr, Cursor cursor) throws
            InternalNakadiException, NoSuchEventTypeException, ServiceUnavailableException, InvalidCursorException;

    String formatOffset(NakadiCursor nakadiCursor);
}
