package org.zalando.nakadi.exceptions.runtime;

public class ZookeeperException extends NakadiRuntimeBaseException {

    public ZookeeperException(final String message, final Throwable cause) {
        super(message, cause);
    }

}