package org.zalando.nakadi.domain;

import java.util.Optional;

public class BatchItemResponse {
    private volatile EventPublishingStatus publishingStatus = EventPublishingStatus.ABORTED;
    private volatile String detail = "";
    private EventPublishingStep step = EventPublishingStep.NONE;
    private String eid = "";
    private Optional<String> partition = Optional.empty();

    public String getEid() {
        return eid;
    }

    public Optional<String> getPartition() {
        return partition;
    }

    public BatchItemResponse setPartition(Optional<String> partition) {
        this.partition = partition;
        return this;
    }

    public BatchItemResponse setEid(final String eid) {
        this.eid = eid;
        return this;
    }

    public EventPublishingStatus getPublishingStatus() {
        return publishingStatus;
    }

    public BatchItemResponse setPublishingStatus(final EventPublishingStatus publishingStatus) {
        this.publishingStatus = publishingStatus;
        return this;
    }

    public EventPublishingStep getStep() {
        return step;
    }

    public BatchItemResponse setStep(final EventPublishingStep step) {
        this.step = step;
        return this;
    }

    public String getDetail() {
        return detail;
    }

    public BatchItemResponse setDetail(final String detail) {
        this.detail = detail;
        return this;
    }
}
