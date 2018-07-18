package org.zalando.nakadi.stream;

import java.util.Set;

public final class StartRequest {

    private Set<String> fromEventTypes;

    private String toEventType;

    public Set<String> getFromEventTypes() {
        return fromEventTypes;
    }

    public void setFromEventTypes(final Set<String> fromEventTypes) {
        this.fromEventTypes = fromEventTypes;
    }

    public String getToEventType() {
        return toEventType;
    }

    public void setToEventType(final String toEventType) {
        this.toEventType = toEventType;
    }
}
