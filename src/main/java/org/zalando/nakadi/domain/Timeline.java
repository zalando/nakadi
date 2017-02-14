package org.zalando.nakadi.domain;

import javax.annotation.Nullable;
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
    private int order;
    private Storage storage;
    private String topic;
    private Date createdAt;
    private Date switchedAt;
    private Date cleanupAt;
    private StoragePosition latestPosition;
    private boolean fake;

    public Timeline(
            final String eventType,
            final int order,
            final Storage storage,
            final String topic,
            final Date createdAt) {
        this.eventType = eventType;
        this.order = order;
        this.storage = storage;
        this.topic = topic;
        this.createdAt = createdAt;
    }

    @Nullable
    public UUID getId() {
        return id;
    }

    public void setId(@Nullable final UUID id) {
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

    @Nullable
    public Date getSwitchedAt() {
        return switchedAt;
    }

    public void setSwitchedAt(@Nullable final Date switchedAt) {
        this.switchedAt = switchedAt;
    }

    @Nullable
    public StoragePosition getLatestPosition() {
        return latestPosition;
    }

    public void setLatestPosition(@Nullable final StoragePosition latestPosition) {
        this.latestPosition = latestPosition;
    }

    @Nullable
    public Date getCleanupAt() {
        return cleanupAt;
    }

    public void setCleanupAt(@Nullable final Date cleanupAt) {
        this.cleanupAt = cleanupAt;
    }

    public boolean isFake() {
        return fake;
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

    public static Timeline createFakeTimeline(final EventType eventType, final Storage storage) {
        final Timeline timeline = new Timeline(eventType.getName(), 0, storage, eventType.getTopic(), new Date());
        timeline.fake = true;
        return timeline;
    }

}
