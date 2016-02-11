package de.zalando.aruha.nakadi.repository;

import de.zalando.aruha.nakadi.NakadiException;

public class TopicCreationException extends NakadiException {
    public TopicCreationException(String msg, Exception cause) {
        super(msg, cause);
    }
}
