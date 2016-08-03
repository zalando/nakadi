package org.zalando.nakadi.domain;

public class EventTypeOptions {

    private Long retentionTime;

    public Long getRetentionTime() {
        return retentionTime;
    }

    public void setRetentionTime(final Long retentionTime) {
        this.retentionTime = retentionTime;
    }
}
