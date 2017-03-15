package org.zalando.nakadi.domain;

public class NakadiCursorDistanceResult {
    private final NakadiCursorDistanceQuery query;
    private long distance;

    public NakadiCursorDistanceResult(final NakadiCursorDistanceQuery query, final long distance) {
        this.query = query;
        this.distance = distance;
    }

    public NakadiCursorDistanceQuery getQuery() {
        return query;
    }

    public long getDistance() {
        return distance;
    }
}
