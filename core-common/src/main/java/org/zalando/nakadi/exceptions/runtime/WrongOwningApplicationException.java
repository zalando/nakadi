package org.zalando.nakadi.exceptions.runtime;

public class WrongOwningApplicationException extends NakadiBaseException {

    public WrongOwningApplicationException(final String owningApplication) {
        super("Owning application is not valid: " + owningApplication);
    }
}
