package org.zalando.nakadi.exceptions.runtime;

public class MyNakadiRuntimeException1 extends RuntimeException {

    public MyNakadiRuntimeException1(final String message) {
        super(message);
    }

    public MyNakadiRuntimeException1(final String message, final Throwable cause) {
        super(message, cause);
    }

}
