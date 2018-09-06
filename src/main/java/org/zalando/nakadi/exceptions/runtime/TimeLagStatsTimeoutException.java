package org.zalando.nakadi.exceptions.runtime;

public class TimeLagStatsTimeoutException extends NakadiBaseException {

    public TimeLagStatsTimeoutException(final String msg, final Throwable cause) {
        super(msg, cause);
    }

}
