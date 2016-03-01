package de.zalando.aruha.nakadi.repository.db;

import de.zalando.aruha.nakadi.domain.EventType;
import de.zalando.aruha.nakadi.exceptions.InternalNakadiException;
import de.zalando.aruha.nakadi.exceptions.NoSuchEventTypeException;
import de.zalando.aruha.nakadi.repository.DuplicatedEventTypeNameException;
import de.zalando.aruha.nakadi.repository.EventTypeRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ExecutionException;

public class CachingEventTypeRepository implements EventTypeRepository {

    private static final Logger LOG = LoggerFactory.getLogger(CachingEventTypeRepository.class);

    private final EventTypeRepository repository;

    private final EventTypeCache cache;

    public CachingEventTypeRepository(final EventTypeRepository repository,
                                      final EventTypeCache cache) throws Exception {
        this.repository = repository;
        this.cache = cache;
    }

    @Override
    public void saveEventType(final EventType eventType) throws InternalNakadiException, DuplicatedEventTypeNameException {
        this.repository.saveEventType(eventType);

        try {
            this.cache.created(eventType.getName());
        } catch (Exception e) {
            LOG.error("Failed to create new cache entry for event type '" + eventType.getName() + "'", e);
            try {
                this.repository.removeEventType(eventType.getName());
            } catch (NoSuchEventTypeException e1) {
                LOG.error("Failed to revert event type db persistence", e1);
            }
            throw new InternalNakadiException("Failed to save event type", e);
        }
    }

    @Override
    public EventType findByName(final String name) throws InternalNakadiException, NoSuchEventTypeException {
        try {
            return cache.get(name);
        } catch (ExecutionException e) {
            LOG.error("Failed to load event type from cache '" + name + "'", e);
            throw new InternalNakadiException("Failed to load event type", e);
        }
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
