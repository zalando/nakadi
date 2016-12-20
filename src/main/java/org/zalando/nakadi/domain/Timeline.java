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

    public static class KafkaStoragePosition implements StoragePosition {
        private List<Long> offsets;

        public List<Long> getOffsets() {
            return offsets;
        }

        public void setOffsets(final List<Long> offsets) {
            this.offsets = offsets;
        }
    }

    private Integer id;
    private String eventType;
    private Integer order;
    private Storage storage;
    private EventTypeConfiguration eventTypeConfiguration;
    private Date createdAt;
    private Date cleanupAt;
    private Date switchedAt;
    private Date freedAt;
    private StoragePosition storagePosition;

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

    public EventTypeConfiguration getEventTypeConfiguration() {
        return eventTypeConfiguration;
    }

    public void setEventTypeConfiguration(final EventTypeConfiguration eventTypeConfiguration) {
        this.eventTypeConfiguration = eventTypeConfiguration;
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

    public StoragePosition getStoragePosition() {
        return storagePosition;
    }

    public void setStoragePosition(final StoragePosition storagePosition) {
        this.storagePosition = storagePosition;
    }

    public Date getCleanupAt() {
        return cleanupAt;
    }

    public void setCleanupAt(final Date cleanupAt) {
        this.cleanupAt = cleanupAt;
    }
}
