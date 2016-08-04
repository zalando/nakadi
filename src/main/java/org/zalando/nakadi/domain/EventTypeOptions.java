package org.zalando.nakadi.domain;

import javax.annotation.Nullable;

public class EventTypeOptions {

    private Long retentionTime;

    public Long getRetentionTime() {
        return retentionTime;
    }

    @Nullable
    public void setRetentionTime(final Long retentionTime) {
        this.retentionTime = retentionTime;
    }
}
