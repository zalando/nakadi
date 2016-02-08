package de.zalando.aruha.nakadi.repository;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import de.zalando.aruha.nakadi.NakadiException;
import de.zalando.aruha.nakadi.domain.EventType;

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

    @Override
    public void update(EventType eventType) throws NakadiException {
        // TODO
    }

    @Override
    public List<EventType> list() {
        return null;
    }
}
