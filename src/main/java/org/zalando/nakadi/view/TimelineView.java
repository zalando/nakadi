package org.zalando.nakadi.view;

import java.util.Date;
import java.util.UUID;
import org.zalando.nakadi.domain.Timeline;

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

    public void setId(UUID id) {
        this.id = id;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public void setOrder(int order) {
        this.order = order;
    }

    public void setStorageId(String storageId) {
        this.storageId = storageId;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public void setCreatedAt(Date createdAt) {
        this.createdAt = createdAt;
    }

    public void setSwitchedAt(Date switchedAt) {
        this.switchedAt = switchedAt;
    }

    public void setCleanedUpAt(Date cleanedUpAt) {
        this.cleanedUpAt = cleanedUpAt;
    }

    public void setLatestPosition(Timeline.StoragePosition latestPosition) {
        this.latestPosition = latestPosition;
    }
}
