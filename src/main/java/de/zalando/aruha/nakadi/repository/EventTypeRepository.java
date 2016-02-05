package de.zalando.aruha.nakadi.repository;

import de.zalando.aruha.nakadi.NakadiException;
import de.zalando.aruha.nakadi.domain.EventType;

public interface EventTypeRepository {

    void saveEventType(EventType eventType) throws NakadiException, DuplicatedEventTypeNameException;

    EventType findByName(String name) throws NakadiException, NoSuchEventTypeException;

    void update(EventType eventType) throws NakadiException;

}
