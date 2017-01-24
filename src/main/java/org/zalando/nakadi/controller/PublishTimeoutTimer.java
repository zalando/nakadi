package org.zalando.nakadi.controller;

public class PublishTimeoutTimer {

    private final long publishTotalTimeout;
    private final long startTime;

    public PublishTimeoutTimer(final long publishTotalTimeout) {
        this.publishTotalTimeout = publishTotalTimeout;
        this.startTime = System.currentTimeMillis();
    }

    public long getTimeLeftMs() {
        final long tillTimeout = startTime + publishTotalTimeout - System.currentTimeMillis();
        return tillTimeout < 0 ? 0 : tillTimeout;
    }
}
