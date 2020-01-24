package org.zalando.nakadi.headers;

import org.zalando.nakadi.domain.BatchItem;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.view.EventOwnerSelector;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class EventHeaderHandlerFactory {

    private static final Map<EventOwnerSelector.Type, EventHeaderHandler> HEADER_HANDLERS;
    static {
        final Map<EventOwnerSelector.Type, EventHeaderHandler> headerMap = new HashMap<>();
        headerMap.put(EventOwnerSelector.Type.PATH, new EventPathHeaderHandler());
        headerMap.put(EventOwnerSelector.Type.STATIC, new EventStaticHeaderHandler());
        HEADER_HANDLERS = Collections.unmodifiableMap(headerMap);
    }

    public void prepare(final BatchItem item, final EventType eventType) {
        if (null != eventType.getEventOwnerSelector()) {
            final EventOwnerSelector selector = eventType.getEventOwnerSelector();
            final EventHeaderHandler eventHeaderHandler = getEventHeaderHandler(selector);
            eventHeaderHandler.prepare(item, selector);
        }
    }

    private EventHeaderHandler getEventHeaderHandler(final EventOwnerSelector selector) {
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
