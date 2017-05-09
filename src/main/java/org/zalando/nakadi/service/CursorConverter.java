package org.zalando.nakadi.service;

import com.google.common.base.Preconditions;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.exceptions.InternalNakadiException;
import org.zalando.nakadi.exceptions.InvalidCursorException;
import org.zalando.nakadi.exceptions.NoSuchEventTypeException;
import org.zalando.nakadi.exceptions.ServiceUnavailableException;
import org.zalando.nakadi.view.Cursor;
import org.zalando.nakadi.view.SubscriptionCursor;
import org.zalando.nakadi.view.SubscriptionCursorWithoutToken;

/**
 * The only place to create NakadiCursor from Cursor or SubscriptionCursor and back.
 */
public interface CursorConverter {

    // Methods to convert from model to view
    Cursor convert(NakadiCursor topicPosition);

    SubscriptionCursor convert(NakadiCursor nakadiCursor, String token);

    // Convert from view to model
    NakadiCursor convert(String eventTypeName, Cursor cursor) throws
            InternalNakadiException, NoSuchEventTypeException, InvalidCursorException, ServiceUnavailableException;

    NakadiCursor convert(SubscriptionCursorWithoutToken cursor) throws
            InternalNakadiException, NoSuchEventTypeException, ServiceUnavailableException, InvalidCursorException;


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
