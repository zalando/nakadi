package org.zalando.nakadi.domain;

public class ShiftedNakadiCursor extends NakadiCursor {
    private long shift = 0;

    public ShiftedNakadiCursor(final Timeline timeline, final String partition, final String offset, final long shift) {
        super(timeline, partition, offset);
        this.shift = shift;
    }

    public long getShift() {
        return shift;
    }
}
