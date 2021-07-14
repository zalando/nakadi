package org.zalando.nakadi.exceptions.runtime;

public class InvalidResourceAnnotationException extends NakadiBaseException {
    private static final String ERROR_MESSAGE_TEMPLATE = "Error validating annotation <%s:%s>; %s";

    public InvalidResourceAnnotationException(final String key, final String value, final String message) {
        super(String.format(ERROR_MESSAGE_TEMPLATE, key, value, message));
    }
}
