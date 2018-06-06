package org.zalando.nakadi.exceptions.runtime;

public class InvalidStorageConfigurationException extends NakadiRuntimeBaseException {

    public InvalidStorageConfigurationException(final String message) {
        super(message);
    }
}
