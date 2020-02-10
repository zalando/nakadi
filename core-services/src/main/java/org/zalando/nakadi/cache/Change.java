package org.zalando.nakadi.cache;

public class Change {
    private final String id;
    private final String eventTypeName;

    public Change(final String id, final String eventTypeName) {
        this.id = id;
        this.eventTypeName = eventTypeName;
    }

    public String getId() {
        return id;
    }

    public String getEventTypeName() {
        return eventTypeName;
    }
}
