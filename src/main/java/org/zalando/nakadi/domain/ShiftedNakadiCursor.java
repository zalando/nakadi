package org.zalando.nakadi.domain;

public class ShiftedNakadiCursor {
    private final NakadiCursor nakadiCursor;
    private final long shift;

    public ShiftedNakadiCursor(final Timeline timeline, final String partition, final String offset, final long shift) {
        this.nakadiCursor = NakadiCursor.of(timeline, partition, offset);
        this.shift = shift;
    }

    public long getShift() {
        return shift;
    }

    public NakadiCursor getNakadiCursor() {
        return nakadiCursor;
    }

    @Override
    public String toString() {
        return "ShiftedNakadiCursor{" +
                nakadiCursor.toString() +
                ", shift='" + shift + '\'' +
                '}';
    }
}
