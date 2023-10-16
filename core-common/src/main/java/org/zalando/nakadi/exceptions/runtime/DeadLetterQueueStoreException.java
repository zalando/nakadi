package org.zalando.nakadi.exceptions.runtime;

public class DeadLetterQueueStoreException extends RuntimeException {

    public DeadLetterQueueStoreException(final String msg) {
        super(msg);
    }

    public DeadLetterQueueStoreException(final String msg, final Throwable cause) {
        super(msg, cause);
    }
}
