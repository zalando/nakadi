package org.zalando.nakadi.exceptions.runtime;

/**
 * Parent class for Nakadi runtime exceptions
 * Name NakadiRuntimeException was already taken for some kind of wrapper. This name is a nice alternative ;)
 */
public class MyNakadiRuntimeException1 extends RuntimeException {

    public MyNakadiRuntimeException1() {
    }

    public MyNakadiRuntimeException1(final String message) {
        super(message);
    }

    public MyNakadiRuntimeException1(final String message, final Throwable cause) {
        super(message, cause);
    }

}
