package org.zalando.nakadi.exceptions.runtime;

public class InvalidStreamIdException extends NakadiBaseException {

    private final String streamId;

    public InvalidStreamIdException(final String message, final String streamId) {
        super(message);
        this.streamId = streamId;
    }

    public String getStreamId() {
        return streamId;
    }
}
