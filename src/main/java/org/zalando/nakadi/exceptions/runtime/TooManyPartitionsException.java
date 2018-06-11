package org.zalando.nakadi.exceptions.runtime;

public class TooManyPartitionsException extends NakadiRuntimeBaseException {

    public TooManyPartitionsException(final String msg) {
        super(msg);
    }

}
