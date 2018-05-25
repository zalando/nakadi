package org.zalando.nakadi.exceptions;

import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.exceptions.runtime.MyNakadiRuntimeException1;

public class ErrorGettingCursorTimeLagException extends MyNakadiRuntimeException1 {

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
