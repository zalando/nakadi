package org.zalando.nakadi.repository.kafka;

import org.apache.commons.lang3.StringUtils;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.exceptions.runtime.InvalidCursorException;

import java.util.Objects;

import static org.zalando.nakadi.domain.CursorError.INVALID_FORMAT;
import static org.zalando.nakadi.domain.CursorError.PARTITION_NOT_FOUND;

public class KafkaCursor implements Comparable<KafkaCursor> {
    private static final int CURSOR_OFFSET_LENGTH = 18;
    private final String topic;
    private final int partition;
    private final long offset;

    public KafkaCursor(final String topic, final int partition, final long offset) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
    }

    public String getTopic() {
        return topic;
    }

    public int getPartition() {
        return partition;
    }

    public long getOffset() {
        return offset;
    }

    public KafkaCursor addOffset(final long toAdd) {
        return new KafkaCursor(topic, partition, offset + toAdd);
    }

    public NakadiCursor toNakadiCursor(final Timeline timeline) {
        return NakadiCursor.of(timeline,
                toNakadiPartition(partition),
                toNakadiOffset(offset));
    }

    public static String toNakadiOffset(final long offset) {
        return offset >= 0 ? StringUtils.leftPad(String.valueOf(offset), CURSOR_OFFSET_LENGTH, '0')
                : String.valueOf(offset);
    }

    public static String toNakadiPartition(final int partition) {
        return String.valueOf(partition);
    }

    public static int toKafkaPartition(final String partition) {
        return Integer.parseInt(partition);
    }

    public static KafkaCursor fromNakadiCursor(final NakadiCursor tp) throws InvalidCursorException {
        final Integer partition;
        try {
            partition = toKafkaPartition(tp.getPartition());
        } catch (final NumberFormatException ex) {
            throw new InvalidCursorException(PARTITION_NOT_FOUND, tp);
        }
        try {
            return new KafkaCursor(tp.getTopic(), partition, toKafkaOffset(tp.getOffset()));
        } catch (final NumberFormatException ex) {
            throw new InvalidCursorException(INVALID_FORMAT, tp);
        }
    }

    public static long toKafkaOffset(final String offset) {
        return Long.parseLong(offset);
    }

    @Override
    public int compareTo(final KafkaCursor other) {
        return Long.compare(getOffset(), other.getOffset());
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof KafkaCursor)) {
            return false;
        }

        final KafkaCursor that = (KafkaCursor) o;

        return Objects.equals(topic, that.topic)
                && partition == that.partition
                && offset == that.offset;
    }

    @Override
    public int hashCode() {
        int result = topic != null ? topic.hashCode() : 0;
        result = 31 * result + partition;
        result = 31 * result + (int) (offset ^ (offset >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "KafkaCursor{" +
                "topic='" + topic + '\'' +
                ", partition=" + partition +
                ", offset=" + offset +
                '}';
    }
}
