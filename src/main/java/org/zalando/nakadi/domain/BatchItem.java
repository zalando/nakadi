package org.zalando.nakadi.domain;

import org.json.JSONObject;

import javax.annotation.Nullable;
import java.util.Optional;

public class BatchItem {
    private final BatchItemResponse response;
    private final JSONObject event;
    private String partition;
    private int brokerId;

    public BatchItem(final JSONObject event) {
        this.response = new BatchItemResponse();
        this.event = event;

        Optional.ofNullable(event.optJSONObject("metadata"))
                .map(e -> e.optString("eid", null))
                .ifPresent(this.response::setEid);
    }

    public JSONObject getEvent() {
        return this.event;
    }

    public void setPartition(final String partition) {
        this.partition = partition;
    }

    @Nullable
    public String getPartition() {
        return partition;
    }

    public void setBrokerId(final int brokerId) {
        this.brokerId = brokerId;
    }

    public int getBrokerId() {
        return brokerId;
    }

    public BatchItemResponse getResponse() {
        return response;
    }

    public void setStep(final EventPublishingStep step) {
        response.setStep(step);
    }

    public EventPublishingStep getStep() {
        return response.getStep();
    }

    public void updateStatusAndDetail(final EventPublishingStatus publishingStatus, final String detail) {
        response.setPublishingStatus(publishingStatus);
        response.setDetail(detail);
    }
}