package org.zalando.nakadi.exceptions.runtime;

public class DuplicatedTimelineException extends NakadiRuntimeBaseException {

    public DuplicatedTimelineException(final String msg, final Exception cause) {
        super(msg, cause);
    }

}
