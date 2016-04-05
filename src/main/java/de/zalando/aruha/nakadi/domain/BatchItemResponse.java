package de.zalando.aruha.nakadi.domain;

public class BatchItemResponse {
    private EventPublishingStatus publishingStatus = EventPublishingStatus.ABORTED;
    private EventPublishingStep step = EventPublishingStep.NONE;
    private String detail = "";

    public EventPublishingStatus getPublishingStatus() {
        return publishingStatus;
    }

    public void setPublishingStatus(EventPublishingStatus publishingStatus) {
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
