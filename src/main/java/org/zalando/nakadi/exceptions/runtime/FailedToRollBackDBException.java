package org.zalando.nakadi.exceptions.runtime;

public class FailedToRollBackDBException extends NakadiBaseException {

    public FailedToRollBackDBException(final String msg, final Exception cause) {
        super(msg, cause);
    }

}
