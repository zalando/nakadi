package de.zalando.aruha.nakadi.repository;

import de.zalando.aruha.nakadi.domain.EventType;
import de.zalando.aruha.nakadi.exceptions.InternalNakadiException;
import de.zalando.aruha.nakadi.exceptions.NoSuchEventTypeException;

import java.util.List;

public interface EventTypeRepository {

    void saveEventType(EventType eventType) throws InternalNakadiException, DuplicatedEventTypeNameException;

    EventType findByName(String name) throws InternalNakadiException, NoSuchEventTypeException;

    void update(EventType eventType) throws InternalNakadiException, NoSuchEventTypeException;

    List<EventType> list();

    void removeEventType(String name) throws InternalNakadiException, NoSuchEventTypeException;
}
