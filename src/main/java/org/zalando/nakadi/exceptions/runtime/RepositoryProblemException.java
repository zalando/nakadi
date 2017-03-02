package org.zalando.nakadi.exceptions.runtime;

public class RepositoryProblemException extends MyNakadiRuntimeException1 {

    public RepositoryProblemException(final String msg, final Exception cause) {
        super(msg, cause);
    }

}
