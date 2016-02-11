package de.zalando.aruha.nakadi.repository.kafka;

import de.zalando.aruha.nakadi.domain.Cursor;

import javax.annotation.concurrent.Immutable;

@Immutable
public class KafkaCursor {

    private final int partition;

    private final long offset;

    private KafkaCursor(final int partition, final long offset) {
        this.partition = partition;
        this.offset = offset;
    }

    public int getPartition() {
        return partition;
    }

    public long getOffset() {
        return offset;
    }

    public Cursor asNakadiCursor() {
        return new Cursor(toNakadiPartition(partition), toNakadiOffset(offset));
    }

    public static KafkaCursor fromNakadiCursor(final Cursor cursor) {
        return kafkaCursor(cursor.getPartition(), cursor.getOffset());
    }

    public static KafkaCursor kafkaCursor(final int partition, final long offset) {
        return new KafkaCursor(partition, offset);
    }

    public static KafkaCursor kafkaCursor(final String partition, final String offset) {
        return new KafkaCursor(toKafkaPartition(partition), toKafkaOffset(offset));
    }

    public static int toKafkaPartition(final String partition) {
        return Integer.parseInt(partition);
    }

    public static String toNakadiPartition(final int partition) {
        return Integer.toString(partition);
    }

    public static long toKafkaOffset(final String offest) {
        return Long.parseLong(offest);
    }

    public static String toNakadiOffset(final long offset) {
        return Long.toString(offset);
    }

}
