package org.zalando.nakadi.view;

public class CursorLag {
    private final String partition;
    private final String oldestAvailableOffset;
    private final String newestAvailableOffset;
    private final long unconsumedEvents;

    public CursorLag(final String partition, final String oldestAvailableOffset,
                     final String newestAvailableOffset, final long unconsumedEvents) {
        this.partition = partition;
        this.oldestAvailableOffset = oldestAvailableOffset;
        this.newestAvailableOffset = newestAvailableOffset;
        this.unconsumedEvents = unconsumedEvents;
    }

    public String getPartition() {
        return partition;
    }

    public String getOldestAvailableOffset() {
        return oldestAvailableOffset;
    }

    public String getNewestAvailableOffset() {
        return newestAvailableOffset;
    }

    public long getUnconsumedEvents() {
        return unconsumedEvents;
    }
}
