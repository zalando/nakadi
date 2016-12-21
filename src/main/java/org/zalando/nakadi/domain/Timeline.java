package org.zalando.nakadi.domain;

import java.util.Date;
import java.util.List;

public class Timeline {
    public interface EventTypeConfiguration {
    }

    public static class KafkaEventTypeConfiguration implements EventTypeConfiguration {
        private String topicName;

        public String getTopicName() {
            return topicName;
        }

        public void setTopicName(final String topicName) {
            this.topicName = topicName;
        }
    }

    public interface StoragePosition {

    }

    private Integer id;
    private String eventType;
    private Integer order;
    private Storage storage;
    private EventTypeConfiguration storageConfiguration;
    private Date createdAt;
    private Date cleanupAt;
    private Date switchedAt;
    private Date freedAt;
    private List<VersionedCursor> lastPosition;

    public Integer getId() {
        return id;
    }

    public void setId(final Integer id) {
        this.id = id;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(final String eventType) {
        this.eventType = eventType;
    }

    public Integer getOrder() {
        return order;
    }

    public void setOrder(final Integer order) {
        this.order = order;
    }

    public Storage getStorage() {
        return storage;
    }

    public void setStorage(final Storage storage) {
        this.storage = storage;
    }

    public EventTypeConfiguration getStorageConfiguration() {
        return storageConfiguration;
    }

    public void setStorageConfiguration(final EventTypeConfiguration storageConfiguration) {
        this.storageConfiguration = storageConfiguration;
    }

    public Date getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(final Date createdAt) {
        this.createdAt = createdAt;
    }

    public Date getSwitchedAt() {
        return switchedAt;
    }

    public void setSwitchedAt(final Date switchedAt) {
        this.switchedAt = switchedAt;
    }

    public Date getFreedAt() {
        return freedAt;
    }

    public void setFreedAt(final Date freedAt) {
        this.freedAt = freedAt;
    }

    public List<VersionedCursor> getLastPosition() {
        return lastPosition;
    }

    public void setLastPosition(final List<VersionedCursor> lastPosition) {
        this.lastPosition = lastPosition;
    }

    public Date getCleanupAt() {
        return cleanupAt;
    }

    public void setCleanupAt(final Date cleanupAt) {
        this.cleanupAt = cleanupAt;
    }
}
