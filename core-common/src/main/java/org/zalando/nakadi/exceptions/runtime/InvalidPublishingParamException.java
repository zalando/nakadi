package org.zalando.nakadi.exceptions.runtime;

public class InvalidPublishingParamException extends NakadiBaseException {
    public InvalidPublishingParamException(final String message) {
        super(message);
    }
}
