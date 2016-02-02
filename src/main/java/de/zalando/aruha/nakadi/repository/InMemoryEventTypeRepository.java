package de.zalando.aruha.nakadi.repository;

import com.sun.istack.internal.Nullable;
import de.zalando.aruha.nakadi.NakadiException;
import de.zalando.aruha.nakadi.domain.EventType;

import java.util.HashMap;
import java.util.Map;

public class InMemoryEventTypeRepository implements EventTypeRepository {
    private final Map<String, EventType> eventTypes = new HashMap<>();

    @Override
    public void saveEventType(final EventType eventType) throws NakadiException {
        eventTypes.put(eventType.getName(), eventType);
    }

    @Override
    @Nullable
    public EventType findByName(final String eventTypeName) throws NoSuchEventTypeException {
        final EventType eventType = eventTypes.get(eventTypeName);
        if (eventType == null) {
            throw new NoSuchEventTypeException("EventType \"" + eventTypeName + "\" does not exist.");
        }
        return eventType;
    }
}
