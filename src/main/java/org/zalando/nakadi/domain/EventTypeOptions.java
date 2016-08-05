package org.zalando.nakadi.domain;

import javax.annotation.Nullable;

public class EventTypeOptions {

    private Long retentionTime;

    @Nullable
    public Long getRetentionTime() {
        return retentionTime;
    }

    public void setRetentionTime(@Nullable final Long retentionTime) {
        this.retentionTime = retentionTime;
    }
}
