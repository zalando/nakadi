package org.zalando.nakadi.domain;

import java.util.List;

public class EventPublishResult {
    private final EventPublishingStatus status;
    private final EventPublishingStep step;
    private final List<BatchItemResponse> responses;

    public EventPublishResult(final EventPublishingStatus status, final EventPublishingStep step,
                              final List<BatchItemResponse> responses) {
        this.status = status;
        this.step = step;
        this.responses = responses;
    }

    public EventPublishingStatus getStatus() {
        return status;
    }

    public EventPublishingStep getStep() {
        return step;
    }

    public List<BatchItemResponse> getResponses() {
        return responses;
    }
}
