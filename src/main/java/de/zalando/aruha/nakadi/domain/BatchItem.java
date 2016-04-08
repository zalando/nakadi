package de.zalando.aruha.nakadi.domain;

import org.json.JSONObject;

import static de.zalando.aruha.nakadi.domain.EventPublishingStatus.FAILED;
import static de.zalando.aruha.nakadi.domain.EventPublishingStatus.SUBMITTED;

public class BatchItem {
    private final BatchItemResponse response;
    private final JSONObject event;
    private String partition;

    public BatchItem(final JSONObject event) {
        this.response = new BatchItemResponse();
        this.event = event;
    }

    public JSONObject getEvent() {
        return this.event;
    }

    public void setPartition(final String partition) {
        this.partition = partition;
    }

    public String getPartition() {
        return this.partition;
    }

    public BatchItemResponse getResponse() {
        return this.response;
    }

    public void setStep(final EventPublishingStep step) {
        this.response.setStep(step);
    }

    synchronized public void updateStatusAndDetail(final EventPublishingStatus publishingStatus, final String detail) {
        this.response.setPublishingStatus(publishingStatus);
        this.response.setDetail(detail);
    }
}