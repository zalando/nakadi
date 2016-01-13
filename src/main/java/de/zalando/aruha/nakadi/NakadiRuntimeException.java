package de.zalando.aruha.nakadi;

import de.zalando.aruha.nakadi.domain.Problem;

public class NakadiRuntimeException extends RuntimeException {

    public NakadiRuntimeException(final String msg) {
        super(msg);
    }

    public NakadiRuntimeException(final String msg, final Exception cause) {
        super(msg, cause);
    }

    public Problem asProblem() {
        return new Problem(getMessage());
    }

}
