package de.zalando.aruha.nakadi.domain;

import org.springframework.beans.factory.annotation.Value;

public class EventTypeOptions {

    @Value("${nakadi.topic.default.retentionMs}")
    private long maxTopicRetentionMs;

    @Value("${nakadi.topic.min.retentionMs}")
    private long minTopicRetentionMs;

    private Long retentionTime;

    public Long getRetentionTime() {
        return retentionTime;
    }

    public void setRetentionTime(Long retentionTime) {
        this.retentionTime = retentionTime;
    }
}
