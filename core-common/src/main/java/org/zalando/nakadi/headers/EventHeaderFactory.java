package org.zalando.nakadi.headers;

import org.zalando.nakadi.domain.BatchItem;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.view.EventOwnerSelector;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class EventHeaderFactory {

    private static final Map<EventOwnerSelector.Type, EventHeader> HEADER_HANDLERS;
    static {
        final Map<EventOwnerSelector.Type, EventHeader> headerMap = new HashMap<>();
        headerMap.put(EventOwnerSelector.Type.PATH, new EventPathHeader());
        headerMap.put(EventOwnerSelector.Type.STATIC, new EventStaticHeader());
        HEADER_HANDLERS = Collections.unmodifiableMap(headerMap);
    }

    public void prepare(final BatchItem item, final EventType eventType) {
        if (null != eventType.getEventOwnerSelector()) {
            final EventOwnerSelector selector = eventType.getEventOwnerSelector();
            final EventHeader eventHeader = getEventHeaderHandler(selector);
            eventHeader.prepare(item, selector);
        }
    }

    private EventHeader getEventHeaderHandler(final EventOwnerSelector selector) {
        switch (selector.getType()) {
            case PATH:
                return HEADER_HANDLERS.get(EventOwnerSelector.Type.PATH);
            case STATIC:
                return HEADER_HANDLERS.get(EventOwnerSelector.Type.STATIC);
            default:
                throw new IllegalStateException("Unsupported Type for event_owner_selector: " + selector.getType());
        }
    }
}
