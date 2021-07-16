package org.zalando.nakadi.exceptions.runtime;

public class InvalidOwningApplicationException extends NakadiBaseException {

    public InvalidOwningApplicationException(final String owningApplication) {
        super("Owning application is not valid: " + owningApplication);
    }
}
