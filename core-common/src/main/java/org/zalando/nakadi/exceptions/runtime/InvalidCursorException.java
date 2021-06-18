package org.zalando.nakadi.exceptions.runtime;

import org.zalando.nakadi.domain.CursorError;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.view.Cursor;

public class InvalidCursorException extends NakadiBaseException {

    private final CursorError error;
    private final Cursor cursor;
    private final NakadiCursor position;
    private final String eventType;

    public InvalidCursorException(final CursorError error, final Cursor cursor, final String eventType) {
        this.error = error;
        this.cursor = cursor;
        this.position = null;
        this.eventType = eventType;
    }

    public InvalidCursorException(final CursorError error, final NakadiCursor position) {
        this.error = error;
        this.cursor = null;
        this.eventType = null;
        this.position = position;
    }

    public InvalidCursorException(final CursorError error, final String eventType) {
        this.error = error;
        this.cursor = null;
        this.position = null;
        this.eventType = eventType;
    }

    public CursorError getError() {
        return error;
    }

    private String getEventType() {
        if (null != position) {
            return position.getEventType();
        } else if (null != eventType) {
            return eventType;
        } else {
            return null;
        }
    }

    private String getPartition() {
        if (null != cursor) {
            return cursor.getPartition();
        } else if (null != position) {
            return position.getPartition();
        } else {
            return null;
        }
    }

    private String getOffset() {
        if (null != cursor) {
            return cursor.getOffset();
        } else if (null != position) {
            return position.getOffset();
        } else {
            return null;
        }
    }

    @Override
    public String getMessage() {
        switch (error) {
            case PARTITION_NOT_FOUND:
                return "non existing partition " + getPartition();
            case UNAVAILABLE:
                return "offset " + getOffset() + " for partition " + getPartition() + " event type " +
                        getEventType() + " is unavailable as retention time of data elapsed. " +
                        "PATCH partition offset with valid and available offset";
            case NULL_OFFSET:
                return "offset must not be null";
            case NULL_PARTITION:
                return "partition must not be null";
            case FORBIDDEN:
                return "invalid stream id";
            case INVALID_FORMAT:
                return "invalid cursor format, partition: " + getPartition() + " offset: " + getOffset();
            case INVALID_OFFSET:
                return "invalid offset " + getOffset() + " for partition " + getPartition();
            default:
                return "invalid offset " + getOffset() + " for partition " + getPartition() + "(" + error.name() + ")";
        }
    }
}
