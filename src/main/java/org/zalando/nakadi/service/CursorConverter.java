package org.zalando.nakadi.service;

import com.google.common.base.Preconditions;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.exceptions.runtime.InternalNakadiException;
import org.zalando.nakadi.exceptions.runtime.InvalidCursorException;
import org.zalando.nakadi.exceptions.runtime.NoSuchEventTypeException;
import org.zalando.nakadi.exceptions.runtime.ServiceTemporarilyUnavailableException;
import org.zalando.nakadi.view.Cursor;
import org.zalando.nakadi.view.SubscriptionCursor;
import org.zalando.nakadi.view.SubscriptionCursorWithoutToken;

import java.util.Collection;
import java.util.List;

/**
 * The only place to create NakadiCursor from Cursor or SubscriptionCursor and back.
 */
public interface CursorConverter {

    // Methods to convert from model to view
    Cursor convert(NakadiCursor topicPosition);

    SubscriptionCursor convert(NakadiCursor nakadiCursor, String token);

    SubscriptionCursorWithoutToken convertToNoToken(NakadiCursor cursor);

    // Convert from view to model
    NakadiCursor convert(String eventTypeName, Cursor cursor) throws
            InternalNakadiException, NoSuchEventTypeException, InvalidCursorException,
            ServiceTemporarilyUnavailableException;

    NakadiCursor convert(SubscriptionCursorWithoutToken cursor) throws
            InternalNakadiException, NoSuchEventTypeException, ServiceTemporarilyUnavailableException,
            InvalidCursorException;

    List<NakadiCursor> convert(Collection<SubscriptionCursorWithoutToken> cursor) throws
            InternalNakadiException, NoSuchEventTypeException, ServiceTemporarilyUnavailableException,
            InvalidCursorException;


    int VERSION_LENGTH = 3;

    enum Version {
        ZERO("000"),
        ONE("001"),;
        public final String code;

        Version(final String code) {
            Preconditions.checkArgument(
                    code.length() == VERSION_LENGTH,
                    "Version field length should be equal to " + VERSION_LENGTH);
            this.code = code;
        }
    }
}
