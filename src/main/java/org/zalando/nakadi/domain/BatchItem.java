package org.zalando.nakadi.domain;

import org.json.JSONObject;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

public class BatchItem {
    private final BatchItemResponse response;
    private final JSONObject event;
    private String partition;
    private String brokerId;
    private int eventSize;

    public BatchItem(final String event) {
        this.event = new JSONObject(event);
        this.eventSize = event.getBytes(StandardCharsets.UTF_8).length;
        this.response = new BatchItemResponse();

        Optional.ofNullable(this.event.optJSONObject("metadata"))
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

    @Nullable
    public String getBrokerId() {
        return brokerId;
    }

    public void setBrokerId(final String brokerId) {
        this.brokerId = brokerId;
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

    public int getEventSize() {
        return eventSize;
    }
}