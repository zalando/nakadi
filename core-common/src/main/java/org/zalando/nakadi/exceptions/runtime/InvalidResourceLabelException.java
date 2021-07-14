package org.zalando.nakadi.exceptions.runtime;

public class InvalidResourceLabelException extends NakadiBaseException {
    private static final String ERROR_MESSAGE_TEMPLATE = "Error validating label <%s:%s>; %s";

    public InvalidResourceLabelException(final String key, final String value, final String message) {
        super(String.format(ERROR_MESSAGE_TEMPLATE, key, value, message));
    }
}
