package org.zalando.nakadi.domain;

import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import javax.annotation.Nullable;
import org.zalando.nakadi.repository.kafka.KafkaCursor;
import org.zalando.nakadi.util.UUIDGenerator;

public class Timeline {

    public static final int STARTING_ORDER = 0;

    public interface StoragePosition {

        NakadiCursor toNakadiCursor(Timeline timeline, String partition);

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
        public NakadiCursor toNakadiCursor(final Timeline timeline, final String partitionStr) {
            final int partition = KafkaCursor.toKafkaPartition(partitionStr);

            final KafkaCursor cursor = new KafkaCursor(timeline.getTopic(), partition, offsets.get(partition));
            return cursor.toNakadiCursor(timeline);
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
    private Date cleanedUpAt;
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

    @Nullable
    public void setId(final UUID id) {
        this.id = id;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(final String eventType) {
        this.eventType = eventType;
    }

    public int getOrder() {
        return order;
    }

    public void setOrder(final int order) {
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
    public NakadiCursor calculateNakadiLatestPosition(final String partition) {
        if (null == latestPosition) {
            return null;
        }
        return latestPosition.toNakadiCursor(this, partition);
    }

    @Nullable
    public Date getCleanedUpAt() {
        return cleanedUpAt;
    }

    public void setCleanedUpAt(@Nullable final Date cleanedUpAt) {
        this.cleanedUpAt = cleanedUpAt;
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
                && Objects.equals(cleanedUpAt, that.cleanedUpAt)
                && Objects.equals(latestPosition, that.latestPosition);
    }

    @Override
    public int hashCode() {
        return id != null ? id.hashCode() : 0;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Timeline{");
        sb.append("id=").append(id);
        sb.append(", eventType='").append(eventType).append('\'');
        sb.append(", order=").append(order);
        sb.append(", storage=").append(storage);
        sb.append(", topic='").append(topic).append('\'');
        sb.append(", createdAt=").append(createdAt);
        sb.append(", switchedAt=").append(switchedAt);
        sb.append(", cleanedUpAt=").append(cleanedUpAt);
        sb.append(", latestPosition=").append(latestPosition);
        sb.append(", fake=").append(fake);
        sb.append('}');
        return sb.toString();
    }

    public static Timeline createTimeline(
            final String eventType,
            final int order,
            final Storage storage,
            final String topic,
            final Date createdAt) {
        final Timeline timeline = new Timeline(eventType, order, storage, topic, createdAt);
        timeline.setId(new UUIDGenerator().randomUUID());
        return timeline;
    }

    private static final Date FAKE_CREATION_TIME = new Date(0L);

    public static Timeline createFakeTimeline(final EventTypeBase eventType, final Storage storage) {
        final Timeline timeline = new Timeline(
                eventType.getName(), STARTING_ORDER, storage, eventType.getTopic(), FAKE_CREATION_TIME);
        timeline.fake = true;
        return timeline;
    }

}
