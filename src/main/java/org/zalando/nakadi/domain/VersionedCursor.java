package org.zalando.nakadi.domain;

import java.util.function.Function;

public abstract class VersionedCursor {
    private final Integer version;
    private final String partition;

    public VersionedCursor(final Integer version, final String partition) {
        this.version = version;
        this.partition = partition;
    }

    public Integer getVersion() {
        return version;
    }

    public String getPartition() {
        return partition;
    }

    public abstract Integer getTimelineId();

    public Cursor toCursor() {
        return new Cursor(
                partition,
                version + "-" + serializeVersionSpecificOffset()
        );
    }

    public static VersionedCursor restore(final Cursor cursor) {
        final String[] offsetData = cursor.getOffset().split("-", 1);
        if (offsetData.length == 1) {
            return new VersionedCursorV0(cursor.getPartition(), offsetData[0]);
        } else {
            switch (Integer.parseInt(offsetData[0])) {
                case VersionedCursorV1.VERSION:
                    return VersionedCursorV1.restoreInternal(cursor.getPartition(), offsetData[1]);
                default:
                    throw new IllegalArgumentException("Cursor " + cursor + " can not be parsed");
            }
        }
    }

    protected abstract String serializeVersionSpecificOffset();

    public static class VersionedCursorV0 extends VersionedCursor {
        private static final int VERSION = 0;
        private final String offset;

        public VersionedCursorV0(final String partition, final String offset) {
            super(VERSION, partition);
            this.offset = offset;
        }

        @Override
        public Integer getTimelineId() {
            return null;
        }

        public String getOffset() {
            return offset;
        }

        @Override
        public Cursor toCursor() {
            return new Cursor(getPartition(), offset);
        }

        @Override
        protected String serializeVersionSpecificOffset() {
            return offset;
        }
    }

    public static class VersionedCursorV1 extends VersionedCursor {
        private static final int VERSION = 1;
        private final Integer timelineId;
        private final String offset;

        public VersionedCursorV1(
                final String partition, final Integer timelineId, final String offset) {
            super(VERSION, partition);
            this.timelineId = timelineId;
            this.offset = offset;
        }

        @Override
        public Integer getTimelineId() {
            return timelineId;
        }

        public String getOffset() {
            return offset;
        }

        public <T> T getOffset(final Function<String, T> converter) {
            return converter.apply(offset);
        }

        @Override
        protected String serializeVersionSpecificOffset() {
            return timelineId + "-" + offset;
        }

        private static VersionedCursorV1 restoreInternal(final String partition, final String versionSpecificOffset) {
            final String[] timelineAndData = versionSpecificOffset.split("-", 1);
            return new VersionedCursorV1(partition, Integer.parseInt(timelineAndData[0]), timelineAndData[1]);
        }
    }
}
