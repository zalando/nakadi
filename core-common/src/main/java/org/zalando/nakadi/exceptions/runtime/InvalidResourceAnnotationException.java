package org.zalando.nakadi.exceptions.runtime;

public class InvalidResourceAnnotationException extends NakadiBaseException {

    public InvalidResourceAnnotationException(final String message) {
        super("InvalidResourceAnnotation: " + message);
    }
}
