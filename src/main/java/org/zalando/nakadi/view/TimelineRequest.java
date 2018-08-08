package org.zalando.nakadi.view;

import javax.annotation.Nullable;

public class TimelineRequest {

    private String storageId;

    @Nullable
    private String topic;

    public String getStorageId() {
        return storageId;
    }

    public void setStorageId(final String storageId) {
        this.storageId = storageId;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(final String topic) {
        this.topic = topic;
    }
}
