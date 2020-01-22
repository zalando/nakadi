package org.zalando.nakadi.domain;

import org.json.JSONObject;
import org.zalando.nakadi.plugin.api.authz.Resource;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;

public class Event {
    private final String rawEvent;
    private final JSONObject event;
    private final int eventSize;
    private String id = "";

    @Nullable
    private EventAuthorization authorization;

    public Event(final String rawEvent) {
        this.event = StrictJsonParser.parseObject(rawEvent);
        this.rawEvent = rawEvent;
        this.eventSize = rawEvent.getBytes(StandardCharsets.UTF_8).length;
    }

    public String getEventString() {
        return rawEvent;
    }

    public JSONObject getEventJson() {
        return event;
    }

    public int getEventSize() {
        return eventSize;
    }

    public String getId() {
        return id;
    }

    public void setAuthorization(final EventAuthorization authorization) {
        this.authorization = authorization;
    }

    public void setId(final String id) {
        this.id = id;
    }

    public EventAuthorization getAuthorization() {
        return this.authorization;
    }

    public Resource<Event> asResource() {
        return new ResourceImpl<>(id, ResourceImpl.EVENT_RESOURCE, getAuthorization(), this);
    }

    @Override
    public String toString() {
        return this.event.toString();
    }

}
