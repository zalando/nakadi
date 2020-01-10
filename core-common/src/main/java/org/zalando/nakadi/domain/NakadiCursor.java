package org.zalando.nakadi.domain;

import com.google.common.base.Preconditions;
import org.zalando.nakadi.exceptions.runtime.InvalidCursorException;
import org.zalando.nakadi.exceptions.runtime.NakadiBaseException;
import org.zalando.nakadi.repository.kafka.KafkaCursor;

import java.util.Objects;

public abstract class NakadiCursor {
    public static final int VERSION_LENGTH = 3;

    /**
     * - ZERO is reserved for old offset format, e.g. those previous to timelines: "000000000000000010"
     * - ONE is reserved for the first version of timeline offsets: "001-0001-0000000000000001"
     **/
    public enum Version {
        ZERO("000"),
        ONE("001"),;
        public final String code;

        Version(final String code) {
            Preconditions.checkArgument(
                    code.length() == VERSION_LENGTH,
                    "Version field length should be equal to " + VERSION_LENGTH);
            this.code = code;
        }
    }

    private final Timeline timeline;
    private final String partition;
    // NO BEGIN HERE - only real offset!
    private final String offset;

    private NakadiCursor(
            final Timeline timeline,
            final String partition,
            final String offset) {
        this.timeline = timeline;
        this.partition = partition;
        this.offset = offset;
    }

    public Timeline getTimeline() {
        return timeline;
    }

    public String getTopic() {
        return timeline.getTopic();
    }

    public String getEventType() {
        return timeline.getEventType();
    }

    public String getPartition() {
        return partition;
    }

    public String getOffset() {
        return offset;
    }

    public EventTypePartition getEventTypePartition() {
        return new EventTypePartition(timeline.getEventType(), partition);
    }

    public TopicPartition getTopicPartition() {
        return new TopicPartition(timeline.getTopic(), partition);
    }

    public abstract NakadiCursor shiftWithinTimeline(long offset);

    public abstract boolean isLast();

    public abstract boolean isInitial();

    public KafkaCursor asKafkaCursor() throws InvalidCursorException {
        throw new UnsupportedOperationException("Cursor of class " + getClass() + " can not be converted to kafka one");
    }

    public abstract void checkStorageAvailability() throws InvalidCursorException;

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof NakadiCursor)) {
            return false;
        }

        final NakadiCursor that = (NakadiCursor) o;
        return Objects.equals(this.timeline, that.timeline)
                && Objects.equals(this.partition, that.partition)
                && Objects.equals(this.offset, that.offset);
    }

    @Override
    public int hashCode() {
        int result = timeline.hashCode();
        result = 31 * result + partition.hashCode();
        result = 31 * result + offset.hashCode();
        return result;
    }

    @Override
    public String toString() {
        //Ok, it's time to compact the message.
        return "T(" + Timeline.debugString(timeline) + ")-" +
                "P(" + partition + ")-" +
                "O(" + offset + ")";
    }


    public static NakadiCursor of(final Timeline timeline, final String partition, final String offset) {
        switch (timeline.getStorage().getType()) {
            case KAFKA:
                return new NakadiKafkaCursor(timeline, partition, offset);
            default:
                throw new NakadiBaseException(
                        "Cursor storage type " + timeline.getStorage().getType() + " not supported");
        }
    }

    private static class NakadiKafkaCursor extends NakadiCursor {
        NakadiKafkaCursor(final Timeline timeline, final String partition, final String offset) {
            super(timeline, partition, offset);
        }

        @Override
        public NakadiCursor shiftWithinTimeline(final long toAdd) {
            return new NakadiKafkaCursor(
                    getTimeline(),
                    getPartition(),
                    KafkaCursor.toNakadiOffset(KafkaCursor.toKafkaOffset(getOffset()) + toAdd)
            );
        }

        @Override
        public KafkaCursor asKafkaCursor() throws InvalidCursorException {
            return KafkaCursor.fromNakadiCursor(this);
        }

        @Override
        public boolean isLast() {
            final Timeline timeline = getTimeline();
            if (null == timeline.getLatestPosition()) {
                return false;
            }
            final int partition = KafkaCursor.toKafkaPartition(getPartition());

            final long existingOffset = ((Timeline.KafkaStoragePosition) timeline.getLatestPosition())
                    .getLastOffsetForPartition(partition);

            final long offset = KafkaCursor.toKafkaOffset(getOffset());

            return offset == existingOffset;
        }

        @Override
        public boolean isInitial() {
            return Long.parseLong(getOffset()) == -1; // Yes, it is always like that for kafka.
        }

        @Override
        public void checkStorageAvailability() throws InvalidCursorException {
            asKafkaCursor();
            if (getTimeline().isDeleted()) {
                throw new InvalidCursorException(CursorError.UNAVAILABLE, this);
            }

        }
    }

}
