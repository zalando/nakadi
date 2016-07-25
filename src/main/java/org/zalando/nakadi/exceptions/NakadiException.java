package org.zalando.nakadi.exceptions;


import org.apache.commons.lang3.StringUtils;
import org.zalando.problem.Problem;

import javax.ws.rs.core.Response;

public abstract class NakadiException extends Exception {

    private String problemMessage;

    public NakadiException(final String message) {
        super(message);
    }

    public NakadiException(final String msg, final Exception cause) {
        super(msg, cause);
    }

    public NakadiException(final String msg, final String problemMessage, final Exception cause) {
        this(msg, cause);
        setProblemMessage(problemMessage);
    }

    public String getProblemMessage() {
        return problemMessage;
    }

    public void setProblemMessage(final String problemMessage) {
        this.problemMessage = problemMessage;
    }

    public Problem asProblem() {
        final String detail = StringUtils.isNotBlank(problemMessage) ? problemMessage : getMessage();
        return Problem.valueOf(getStatus(), detail);
    }

    protected abstract Response.StatusType getStatus();

}
