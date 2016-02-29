package de.zalando.aruha.nakadi.repository;

import de.zalando.aruha.nakadi.domain.EventType;
import de.zalando.aruha.nakadi.exceptions.NoSuchEventTypeException;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InMemoryEventTypeRepository implements EventTypeRepository {
    private final Map<String, EventType> eventTypes = new HashMap<>();

    @Override
    public void saveEventType(final EventType eventType) {
        eventTypes.put(eventType.getName(), eventType);
    }

    @Override
    @Nullable
    public EventType findByName(final String eventTypeName) throws NoSuchEventTypeException {
        final EventType eventType = eventTypes.get(eventTypeName);
        if (eventType == null) {
            throw new NoSuchEventTypeException("EventType '" + eventTypeName + "' does not exist.");
        }

        return eventType;
    }

    @Override
    public void update(EventType eventType) {
        // TODO
    }

    @Override
    public List<EventType> list() {
        return null;
    }

    @Override
    public void removeEventType(String name) {
        // TODO
    }
}
