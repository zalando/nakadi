package org.zalando.nakadi.repository.db;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.annotations.DB;
import org.zalando.nakadi.cache.EventTypeCache;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.EventTypeBase;
import org.zalando.nakadi.exceptions.runtime.DuplicatedEventTypeNameException;
import org.zalando.nakadi.exceptions.runtime.InternalNakadiException;
import org.zalando.nakadi.exceptions.runtime.NoSuchEventTypeException;
import org.zalando.nakadi.repository.EventTypeRepository;

import java.util.List;

@Primary
@Component
public class CachingEventTypeRepository implements EventTypeRepository {

    private final EventTypeRepository repository;

    private final EventTypeCache cache;

    @Autowired
    public CachingEventTypeRepository(@DB final EventTypeRepository repository, final EventTypeCache cache) {
        this.repository = repository;
        this.cache = cache;
    }

    @Override
    public EventType saveEventType(final EventTypeBase eventTypeBase) throws InternalNakadiException,
            DuplicatedEventTypeNameException {
        return this.repository.saveEventType(eventTypeBase);
    }

    @Override
    public EventType findByName(final String name) throws InternalNakadiException, NoSuchEventTypeException {
        return cache.getEventType(name);
    }

    @Override
    public void update(final EventType eventType) throws InternalNakadiException, NoSuchEventTypeException {
        this.repository.findByName(eventType.getName());
        this.repository.update(eventType);
    }

    @Override
    public EventType findByNameSynced(final String name) throws InternalNakadiException, NoSuchEventTypeException {
        return this.repository.findByName(name);
    }

    @Override
    public List<EventType> list() {
        return this.repository.list();
    }

    @Override
    public void removeEventType(final String name) throws InternalNakadiException, NoSuchEventTypeException {
        this.repository.findByName(name);
        this.repository.removeEventType(name);
    }

    @Override
    public void notifyUpdated(final String name) {
        this.cache.invalidate(name);
    }
}
