package de.zalando.aruha.nakadi.repository.db;

import de.zalando.aruha.nakadi.domain.EventType;
import de.zalando.aruha.nakadi.exceptions.NakadiException;
import de.zalando.aruha.nakadi.exceptions.NoSuchEventTypeException;
import de.zalando.aruha.nakadi.repository.EventTypeRepository;

import java.util.List;

public class EventTypeCachedRepository implements EventTypeRepository {

    private final EventTypeRepository repository;

    private final EventTypeCache cache;

    public EventTypeCachedRepository(final EventTypeRepository repository,
                                     final EventTypeCache cache) throws Exception {
        this.repository = repository;
        this.cache = cache;
    }

    @Override
    public void saveEventType(final EventType eventType) throws NakadiException {
        this.repository.saveEventType(eventType);
        this.cache.created(eventType);
    }

    @Override
    public EventType findByName(final String name) throws NoSuchEventTypeException {
        return cache.get(name);
    }

    @Override
    public void update(final EventType eventType) throws NakadiException {
        this.repository.update(eventType);
        this.cache.updated(eventType.getName());
    }

    @Override
    public List<EventType> list() {
        return this.repository.list();
    }

    @Override
    public void removeEventType(final String name) {
        this.repository.removeEventType(name);
        this.cache.removed(name);
    }
}
