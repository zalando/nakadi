package org.zalando.nakadi.cache;

import java.util.Date;

public class Change {
    private final String id;
    private final String eventTypeName;
    private final Date occurredAt;

    public Change(final String id, final String eventTypeName, final Date occurredAt) {
        this.id = id;
        this.eventTypeName = eventTypeName;
        this.occurredAt = occurredAt;
    }

    public String getId() {
        return id;
    }

    public String getEventTypeName() {
        return eventTypeName;
    }

    public Date getOccurredAt() {
        return occurredAt;
    }
}
