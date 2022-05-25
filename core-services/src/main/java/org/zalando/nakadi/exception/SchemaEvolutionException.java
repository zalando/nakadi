package org.zalando.nakadi.exception;

import org.zalando.nakadi.exceptions.runtime.NakadiBaseException;

public class SchemaEvolutionException extends NakadiBaseException {
    public SchemaEvolutionException(final String message) {
        super(message);
    }
}
