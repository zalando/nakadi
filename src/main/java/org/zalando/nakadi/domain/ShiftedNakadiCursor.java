package org.zalando.nakadi.domain;

public class ShiftedNakadiCursor extends NakadiCursor {
    private final long shift;

    public ShiftedNakadiCursor(final Timeline timeline, final String partition, final String offset, final long shift) {
        super(timeline, partition, offset);
        this.shift = shift;
    }

    public long getShift() {
        return shift;
    }

    @Override
    public String toString() {
        return "ShiftedNakadiCursor{" +
                "partition='" + getPartition() + '\'' +
                ", offset='" + getOffset() + '\'' +
                ", shift='" + shift + '\'' +
                ", timeline='" + getTimeline() + '\'' +
                '}';
    }
}
