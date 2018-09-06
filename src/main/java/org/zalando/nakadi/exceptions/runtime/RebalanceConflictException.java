package org.zalando.nakadi.exceptions.runtime;

public class RebalanceConflictException extends NakadiBaseException {

    public RebalanceConflictException(final String msg) {
        super(msg);
    }

}
