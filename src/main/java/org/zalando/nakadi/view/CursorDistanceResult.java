package org.zalando.nakadi.view;

public class CursorDistanceResult {

    private Cursor initialCursor;
    private Cursor finalCursor;
    private long distance;

    public Cursor getInitialCursor() {
        return initialCursor;
    }

    public void setInitialCursor(final Cursor initialCursor) {
        this.initialCursor = initialCursor;
    }

    public Cursor getFinalCursor() {
        return finalCursor;
    }

    public void setFinalCursor(final Cursor finalCursor) {
        this.finalCursor = finalCursor;
    }

    public long getDistance() {
        return distance;
    }

    public void setDistance(final long distance) {
        this.distance = distance;
    }
}
