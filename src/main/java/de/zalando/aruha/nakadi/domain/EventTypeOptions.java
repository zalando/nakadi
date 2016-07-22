package de.zalando.aruha.nakadi.domain;

import javax.annotation.Nullable;

public class EventTypeOptions {

    @Nullable
    private Long retentionTime;

    public Long getRetentionTime() {
        return retentionTime;
    }

    public void setRetentionTime(final Long retentionTime) {
        this.retentionTime = retentionTime;
    }
}
