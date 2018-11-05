package org.zalando.nakadi.exceptions.runtime;

import org.zalando.nakadi.domain.NakadiCursor;

public class ErrorGettingCursorTimeLagException extends NakadiBaseException {

    private final NakadiCursor failedCursor;

    public ErrorGettingCursorTimeLagException(final NakadiCursor failedCursor,
                                              final Throwable cause) {
        super("Error occurred when getting subscription time lag as as subscription cursor is wrong or expired: " +
                failedCursor.toString(), cause);
        this.failedCursor = failedCursor;
    }

    public NakadiCursor getFailedCursor() {
        return failedCursor;
    }
}
