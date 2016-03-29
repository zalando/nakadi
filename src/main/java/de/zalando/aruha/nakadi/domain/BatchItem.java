package de.zalando.aruha.nakadi.domain;

import org.json.JSONObject;

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

    public void setPublishingStatus(final EventPublishingStatus publishingStatus) {
        this.response.setPublishingStatus(publishingStatus);
    }

    public void setDetail(String detail) {
        this.response.setDetail(detail);
    }
}