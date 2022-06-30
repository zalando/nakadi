package org.zalando.nakadi.exceptions.runtime;

public class EventPublishingException extends NakadiBaseException {

    public EventPublishingException(
            final String msg, final Exception cause, final String topicId, final String eventType) {
        super(makeMessage(msg, topicId, eventType), cause);
    }

    public EventPublishingException(final String msg, final String topicId, final String eventType) {
        super(makeMessage(msg, topicId, eventType));
    }

    private static String makeMessage(final String msg, final String topicId, final String eventType) {
        return String.format("%s: topic %s / %s", msg, topicId, eventType);
    }
}
