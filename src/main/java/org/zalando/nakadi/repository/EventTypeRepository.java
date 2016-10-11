package org.zalando.nakadi.repository;

import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.exceptions.DuplicatedEventTypeNameException;
import org.zalando.nakadi.exceptions.InternalNakadiException;
import org.zalando.nakadi.exceptions.NoSuchEventTypeException;

import java.util.List;
import java.util.Optional;

public interface EventTypeRepository {

    void saveEventType(EventType eventType) throws InternalNakadiException, DuplicatedEventTypeNameException;

    EventType findByName(String name) throws InternalNakadiException, NoSuchEventTypeException;

    void update(EventType eventType) throws InternalNakadiException, NoSuchEventTypeException;

    List<EventType> list();

    void archiveEventType(String name) throws InternalNakadiException, NoSuchEventTypeException;

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
