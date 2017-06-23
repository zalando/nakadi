package org.zalando.nakadi.exceptions;

import org.zalando.nakadi.exceptions.runtime.MyNakadiRuntimeException1;

public class InvalidStreamIdException extends MyNakadiRuntimeException1 {

    private final String streamId;

    public InvalidStreamIdException(final String message, final String streamId) {
        super(message);
        this.streamId = streamId;
    }

    public String getStreamId() {
        return streamId;
    }
}
