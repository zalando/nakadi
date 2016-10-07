package org.zalando.nakadi.exceptions;

import org.zalando.nakadi.domain.Cursor;
import org.zalando.nakadi.domain.CursorError;

public class InvalidCursorException extends Exception {

    private final CursorError error;
    private final Cursor cursor;

    public InvalidCursorException(final CursorError error, final Cursor cursor) {
        super();
        this.error = error;
        this.cursor = cursor;
    }

    public CursorError getError() {
        return error;
    }

    public Cursor getCursor() {
        return cursor;
    }

    @Override
    public String getMessage() {
        switch (error) {
            case PARTITION_NOT_FOUND:
                return "non existing partition " + cursor.getPartition();
            case EMPTY_PARTITION:
                return "partition " + cursor.getPartition() + " is empty";
            case UNAVAILABLE:
                return "offset " + cursor.getOffset() + " for partition " + cursor.getPartition() + " is unavailable";
            case NULL_OFFSET:
                return "offset must not be null";
            case NULL_PARTITION:
                return "partition must not be null";
            case FORBIDDEN:
                return "invalid stream id";
            default:
                return "invalid offset " + cursor.getOffset() + " for partition " + cursor.getPartition();
        }
    }
}
