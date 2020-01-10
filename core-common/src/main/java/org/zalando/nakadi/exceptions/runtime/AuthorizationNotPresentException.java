package org.zalando.nakadi.exceptions.runtime;

public class AuthorizationNotPresentException extends NakadiBaseException {

    public AuthorizationNotPresentException(final String message) {
        super(message);
    }
}
