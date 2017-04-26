package org.zalando.nakadi.domain;

public class NakadiCursorLag {
    private final NakadiCursor firstCursor;
    private final NakadiCursor lastCursor;
    private long lag;

    public NakadiCursorLag(final NakadiCursor first, final NakadiCursor last) {
        this.firstCursor = first;
        this.lastCursor = last;
    }

    public String getTopic() {
        return firstCursor.getTopic();
    }

    public String getPartition() {
        return firstCursor.getPartition();
    }

    public NakadiCursor getFirstCursor() {
        return firstCursor;
    }

    public NakadiCursor getLastCursor() {
        return lastCursor;
    }

    public long getLag() {
        return lag;
    }

    public void setLag(final long lag) {
        this.lag = lag;
    }
}
