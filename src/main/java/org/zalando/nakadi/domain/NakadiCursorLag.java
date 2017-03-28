package org.zalando.nakadi.domain;

public class NakadiCursorLag {
    private String topic;
    private String partition;
    private NakadiCursor firstCursor;
    private NakadiCursor lastCursor;
    private long lag;

    public NakadiCursorLag(final String topic, final String partition, final NakadiCursor first,
                           final NakadiCursor last) {
        this.topic = topic;
        this.partition = partition;
        this.firstCursor = first;
        this.lastCursor = last;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(final String topic) {
        this.topic = topic;
    }

    public String getPartition() {
        return partition;
    }

    public void setPartition(final String partition) {
        this.partition = partition;
    }

    public NakadiCursor getFirstCursor() {
        return firstCursor;
    }

    public void setFirstCursor(final NakadiCursor firstCursor) {
        this.firstCursor = firstCursor;
    }

    public NakadiCursor getLastCursor() {
        return lastCursor;
    }

    public void setLastCursor(final NakadiCursor lastCursor) {
        this.lastCursor = lastCursor;
    }

    public long getLag() {
        return lag;
    }

    public void setLag(final long lag) {
        this.lag = lag;
    }
}
