package org.zalando.nakadi.domain;

public class NakadiCursorDistanceQuery {
    private final NakadiCursor finalCursor;
    private final NakadiCursor initialCursor;

    public NakadiCursorDistanceQuery(final NakadiCursor initialCursor, final NakadiCursor finalCursor) {
        this.initialCursor = initialCursor;
        this.finalCursor = finalCursor;
    }

    public NakadiCursor getInitialCursor() {
        return initialCursor;
    }

    public NakadiCursor getFinalCursor() {
        return finalCursor;
    }
}
