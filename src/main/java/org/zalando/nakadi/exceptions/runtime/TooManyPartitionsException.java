package org.zalando.nakadi.exceptions.runtime;

public class TooManyPartitionsException extends MyNakadiRuntimeException1 {

    public TooManyPartitionsException(final String msg) {
        super(msg);
    }

}
