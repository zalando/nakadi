package org.zalando.nakadi.domain;

public class BatchItemResponse {
    private EventPublishingStatus publishingStatus = EventPublishingStatus.ABORTED;
    private EventPublishingStep step = EventPublishingStep.NONE;
    private String detail = "";
    private String eid = "";

    public String getEid() {
        return eid;
    }

    public void setEid(final String eid) {
        this.eid = eid;
    }

    public EventPublishingStatus getPublishingStatus() {
        return publishingStatus;
    }

    public void setPublishingStatus(final EventPublishingStatus publishingStatus) {
        this.publishingStatus = publishingStatus;
    }

    public EventPublishingStep getStep() {
        return step;
    }

    public void setStep(final EventPublishingStep step) {
        this.step = step;
    }

    public String getDetail() {
        return detail;
    }

    public void setDetail(final String detail) {
        this.detail = detail;
    }
}
