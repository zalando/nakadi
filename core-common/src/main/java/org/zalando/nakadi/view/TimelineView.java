package org.zalando.nakadi.view;

import org.zalando.nakadi.domain.Timeline;

import java.util.Date;
import java.util.UUID;

public class TimelineView {

    private UUID id;
    private String eventType;
    private int order;
    private String storageId;
    private String topic;
    private Date createdAt;
    private Date switchedAt;
    private Date cleanedUpAt;
    private Timeline.StoragePosition latestPosition;

    public TimelineView() {
    }

    public TimelineView(final Timeline timeline) {
        this.id = timeline.getId();
        this.eventType = timeline.getEventType();
        this.order = timeline.getOrder();
        this.storageId = timeline.getStorage().getId();
        this.topic = timeline.getTopic();
        this.createdAt = timeline.getCreatedAt();
        this.switchedAt = timeline.getSwitchedAt();
        this.cleanedUpAt = timeline.getCleanedUpAt();
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

    public Date getCleanedUpAt() {
        return cleanedUpAt;
    }

    public Timeline.StoragePosition getLatestPosition() {
        return latestPosition;
    }
}
