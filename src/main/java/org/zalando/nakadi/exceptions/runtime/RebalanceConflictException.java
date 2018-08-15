package org.zalando.nakadi.exceptions.runtime;

public class RebalanceConflictException extends NakadiRuntimeBaseException {

    public RebalanceConflictException(final String msg) {
        super(msg);
    }

}
