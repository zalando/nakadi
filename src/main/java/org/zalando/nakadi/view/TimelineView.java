package org.zalando.nakadi.view;

import org.zalando.nakadi.domain.Timeline;

import java.util.Date;
import java.util.UUID;

public class TimelineView {

    private final UUID id;
    private final String eventType;
    private final int order;
    private final String storageId;
    private final String topic;
    private final Date createdAt;
    private final Date switchedAt;
    private final Date cleanupAt;
    private final Timeline.StoragePosition latestPosition;

    public TimelineView(final Timeline timeline) {
        this.id = timeline.getId();
        this.eventType = timeline.getEventType();
        this.order = timeline.getOrder();
        this.storageId = timeline.getStorage().getId();
        this.topic = timeline.getTopic();
        this.createdAt = timeline.getCreatedAt();
        this.switchedAt = timeline.getSwitchedAt();
        this.cleanupAt = timeline.getCleanupAt();
        this.latestPosition = timeline.getLatestPosition();
    }

    public UUID getId() {
        return id;
    }

    public String getEventType() {
        return eventType;
    }

    public int getOrder() {
        return order;
    }

    public String getStorageId() {
        return storageId;
    }

    public String getTopic() {
        return topic;
    }

    public Date getCreatedAt() {
        return createdAt;
    }

    public Date getSwitchedAt() {
        return switchedAt;
    }

    public Date getCleanupAt() {
        return cleanupAt;
    }

    public Timeline.StoragePosition getLatestPosition() {
        return latestPosition;
    }
}
