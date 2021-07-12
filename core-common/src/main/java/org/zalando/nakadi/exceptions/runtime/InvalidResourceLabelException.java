package org.zalando.nakadi.exceptions.runtime;

public class InvalidResourceLabelException extends NakadiBaseException {

    public InvalidResourceLabelException(final String message) {
        super("InvalidResourceLabel: " + message);
    }
}
