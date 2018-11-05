package org.zalando.nakadi.exceptions.runtime;

public class ZookeeperException extends NakadiBaseException {

    public ZookeeperException(final String message, final Throwable cause) {
        super(message, cause);
    }

}