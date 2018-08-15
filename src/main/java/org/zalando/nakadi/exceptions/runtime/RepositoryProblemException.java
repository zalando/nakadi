package org.zalando.nakadi.exceptions.runtime;

public class RepositoryProblemException extends NakadiRuntimeBaseException {

    public RepositoryProblemException(final String msg, final Exception cause) {
        super(msg, cause);
    }

}
