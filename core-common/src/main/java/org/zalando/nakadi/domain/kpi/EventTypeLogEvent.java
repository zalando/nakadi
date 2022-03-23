package org.zalando.nakadi.domain.kpi;

import org.zalando.nakadi.domain.CompatibilityMode;
import org.zalando.nakadi.domain.EventCategory;

public class EventTypeLogEvent {
    private String eventType;
    private String status;
    private EventCategory category;
    private String authz;
    private CompatibilityMode compatibilityMode;


    public String getEventType() {
        return eventType;
    }

    public EventTypeLogEvent setEventType(final String eventType) {
        this.eventType = eventType;
        return this;
    }

    public String getStatus() {
        return status;
    }

    public EventTypeLogEvent setStatus(final String status) {
        this.status = status;
        return this;
    }

    public EventCategory getCategory() {
        return category;
    }

    public EventTypeLogEvent setCategory(final EventCategory category) {
        this.category = category;
        return this;
    }

    public String getAuthz() {
        return authz;
    }

    public EventTypeLogEvent setAuthz(final String authz) {
        this.authz = authz;
        return this;
    }

    public CompatibilityMode getCompatibilityMode() {
        return compatibilityMode;
    }

    public EventTypeLogEvent setCompatibilityMode(final CompatibilityMode compatibilityMode) {
        this.compatibilityMode = compatibilityMode;
        return this;
    }
}
