package org.zalando.nakadi.domain;

public enum UnprocessableEventPolicy {
    SKIP_EVENT,
    DEAD_LETTER_QUEUE
}
