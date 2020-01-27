package org.zalando.nakadi.exceptions.runtime;

// checked exception
public class JsonPathAccessException extends Exception {
    public JsonPathAccessException(final String message) {
        super(message);
    }
}
