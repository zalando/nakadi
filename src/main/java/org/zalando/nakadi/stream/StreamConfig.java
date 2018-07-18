package org.zalando.nakadi.stream;

import java.util.Set;

public final class StreamConfig {

    private Set<String> fromEventTypes;
    private String toEventType;
    private String applicationId;

    private StreamConfig() {
    }

    public static StreamConfig newStreamConfig() {
        return new StreamConfig();
    }

    public Set<String> getFromEventTypes() {
        return fromEventTypes;
    }

    public StreamConfig setFromEventTypes(final Set<String> fromEventTypes) {
        this.fromEventTypes = fromEventTypes;
        return this;
    }

    public String getToEventType() {
        return toEventType;
    }

    public StreamConfig setToEventType(final String toEventType) {
        this.toEventType = toEventType;
        return this;
    }

    public String getApplicationId() {
        return applicationId;
    }

    public StreamConfig setApplicationId(final String applicationId) {
        this.applicationId = applicationId;
        return this;
    }
}
