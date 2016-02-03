package de.zalando.aruha.nakadi;


public class NakadiException extends Exception {

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

}
