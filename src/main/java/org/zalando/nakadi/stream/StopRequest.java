package org.zalando.nakadi.stream;

public class StopRequest {

    private String streamId;

    public StopRequest(final String streamId) {
        this.streamId = streamId;
    }

    public StopRequest() {

    }

    public String getStreamId() {
        return streamId;
    }

    public void setStreamId(final String streamId) {
        this.streamId = streamId;
    }
}
