package org.zalando.nakadi.exceptions.runtime;

public class RepositoryProblemException extends NakadiBaseException {

    public RepositoryProblemException(final String msg, final Exception cause) {
        super(msg, cause);
    }

}
