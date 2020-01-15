package org.zalando.nakadi.domain;

public class NakadiCursorLag {
    private final NakadiCursor firstCursor;
    private final NakadiCursor lastCursor;
    private final long lag;

    public NakadiCursorLag(final NakadiCursor first, final NakadiCursor last, final long lag) {
        this.firstCursor = first;
        this.lastCursor = last;
        this.lag = lag;
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

}
