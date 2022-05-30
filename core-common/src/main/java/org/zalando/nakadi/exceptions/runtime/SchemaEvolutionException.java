package org.zalando.nakadi.exceptions.runtime;

public class SchemaEvolutionException extends NakadiBaseException {
    public SchemaEvolutionException(final String message) {
        super(message);
    }
}
