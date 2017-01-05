package org.zalando.nakadi.domain;

import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

public class Timeline {

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

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof KafkaStoragePosition)) {
                return false;
            }

            final KafkaStoragePosition that = (KafkaStoragePosition) o;

            return offsets != null ? offsets.equals(that.offsets) : that.offsets == null;
        }

        @Override
        public int hashCode() {
            return offsets != null ? offsets.hashCode() : 0;
        }
    }

    private UUID id;
    private String eventType;
    private Integer order;
    private Storage storage;
    private String topic;
    private Date createdAt;
    private Date switchedAt;
    private Date cleanupAt;
    private StoragePosition latestPosition;

    public UUID getId() {
        return id;
    }

    public void setId(final UUID id) {
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

    public String getTopic() {
        return topic;
    }

    public void setTopic(final String topic) {
        this.topic = topic;
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

    public StoragePosition getLatestPosition() {
        return latestPosition;
    }

    public void setLatestPosition(final StoragePosition latestPosition) {
        this.latestPosition = latestPosition;
    }

    public Date getCleanupAt() {
        return cleanupAt;
    }

    public void setCleanupAt(final Date cleanupAt) {
        this.cleanupAt = cleanupAt;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Timeline)) {
            return false;
        }

        final Timeline that = (Timeline) o;

        return Objects.equals(id, that.id)
                && Objects.equals(eventType, that.eventType)
                && Objects.equals(order, that.order)
                && Objects.equals(storage, that.storage)
                && Objects.equals(topic, that.topic)
                && Objects.equals(createdAt, that.createdAt)
                && Objects.equals(switchedAt, that.switchedAt)
                && Objects.equals(cleanupAt, that.cleanupAt)
                && Objects.equals(latestPosition, that.latestPosition);
    }

    @Override
    public int hashCode() {
        return id != null ? id.hashCode() : 0;
    }
}
