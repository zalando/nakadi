package org.zalando.nakadi.exceptions;

import org.zalando.nakadi.domain.CursorError;
import org.zalando.nakadi.domain.TopicPosition;
import org.zalando.nakadi.view.Cursor;

public class InvalidCursorException extends Exception {

    private final CursorError error;
    private final Cursor cursor;
    private final TopicPosition position;

    public InvalidCursorException(final CursorError error, final Cursor cursor) {
        super();
        this.error = error;
        this.cursor = cursor;
        this.position = null;
    }

    public InvalidCursorException(final CursorError error, final TopicPosition position) {
        super();
        this.error = error;
        this.cursor = null;
        this.position = position;
    }

    public InvalidCursorException(final CursorError error) {
        super();
        this.error = error;
        this.cursor = null;
        this.position = null;
    }

    public CursorError getError() {
        return error;
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
                return "offset " + getOffset() + " for partition " + getPartition() + " is unavailable";
            case NULL_OFFSET:
                return "offset must not be null";
            case NULL_PARTITION:
                return "partition must not be null";
            case FORBIDDEN:
                return "invalid stream id";
            case INVALID_FORMAT:
                return "invalid cursor format";
            case INVALID_OFFSET:
                return "invalid offset " + getOffset() + " for partition " + getPartition();
            default:
                return "invalid offset " + getOffset() + " for partition " + getPartition() + "(" + error.name() + ")";
        }
    }
}
