package org.zalando.nakadi.repository;

import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.EventTypeBase;
import org.zalando.nakadi.exceptions.runtime.DuplicatedEventTypeNameException;
import org.zalando.nakadi.exceptions.runtime.InternalNakadiException;
import org.zalando.nakadi.exceptions.runtime.NoSuchEventTypeException;

import java.util.List;
import java.util.Optional;

public interface EventTypeRepository {

    EventType saveEventType(EventTypeBase eventType) throws InternalNakadiException, DuplicatedEventTypeNameException;

    EventType findByName(String name) throws InternalNakadiException, NoSuchEventTypeException;

    void update(EventType eventType) throws InternalNakadiException, NoSuchEventTypeException;

    List<EventType> list();

    void removeEventType(String name) throws InternalNakadiException, NoSuchEventTypeException;

    void notifyUpdated(String name);

    default Optional<EventType> findByNameO(final String eventTypeName) throws InternalNakadiException {
        try {
            return Optional.of(findByName(eventTypeName));
        } catch (final NoSuchEventTypeException e) {
            return Optional.empty();
        } catch (final InternalNakadiException e) {
            throw e;
        }
    }
}
