package org.zalando.nakadi.repository.db;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.annotations.DB;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.EventTypeBase;
import org.zalando.nakadi.exceptions.runtime.InternalNakadiException;
import org.zalando.nakadi.exceptions.runtime.NoSuchEventTypeException;
import org.zalando.nakadi.exceptions.runtime.DuplicatedEventTypeNameException;
import org.zalando.nakadi.repository.EventTypeRepository;

import java.util.List;

@Primary
@Component
public class CachingEventTypeRepository implements EventTypeRepository {

    private static final Logger LOG = LoggerFactory.getLogger(CachingEventTypeRepository.class);

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
        final EventType eventType = this.repository.saveEventType(eventTypeBase);

        try {
            this.cache.created(eventTypeBase.getName());
            return eventType;
        } catch (Exception e) {
            LOG.error("Failed to create new cache entry for event type '" + eventTypeBase.getName() + "'", e);
            try {
                this.repository.removeEventType(eventTypeBase.getName());
            } catch (NoSuchEventTypeException e1) {
                LOG.error("Failed to revert event type db persistence", e1);
            }
            throw new InternalNakadiException("Failed to save event type", e);
        }
    }

    @Override
    public EventType findByName(final String name) throws InternalNakadiException, NoSuchEventTypeException {
        return cache.getEventType(name);
    }

    @Override
    public void update(final EventType eventType) throws InternalNakadiException, NoSuchEventTypeException {
        final EventType original = this.repository.findByName(eventType.getName());
        this.repository.update(eventType);

        try {
            this.cache.updated(eventType.getName());
        } catch (Exception e) {
            LOG.error("Failed to update cache for event type '" + eventType.getName() + "'", e);
            this.repository.update(original);
            throw new InternalNakadiException("Failed to update event type", e);
        }
    }

    @Override
    public List<EventType> list() {
        return this.repository.list();
    }

    @Override
    public void removeEventType(final String name) throws InternalNakadiException, NoSuchEventTypeException {
        final EventType original = this.repository.findByName(name);

        this.repository.removeEventType(name);

        try {
            this.cache.removed(name);
        } catch (Exception e) {
            LOG.error("Failed to remove entry from cache '" + name + "'");
            try {
                this.repository.saveEventType(original);
            } catch (DuplicatedEventTypeNameException e1) {
                LOG.error("Failed to rollback db removal", e);
            }
            throw new InternalNakadiException("Failed to remove event type", e);
        }

    }
}
