package org.zalando.nakadi.exceptions.runtime;

public class TooManyPartitionsException extends NakadiBaseException {

    public TooManyPartitionsException(final String msg) {
        super(msg);
    }

}
