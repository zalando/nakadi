package de.zalando.aruha.nakadi.repository;

import de.zalando.aruha.nakadi.exceptions.NakadiException;
import de.zalando.aruha.nakadi.domain.EventType;
import de.zalando.aruha.nakadi.exceptions.NoSuchEventTypeException;

import java.util.List;

public interface EventTypeRepository {

    void saveEventType(EventType eventType) throws NakadiException;

    EventType findByName(String name) throws NoSuchEventTypeException;

    void update(EventType eventType) throws NakadiException;

    List<EventType> list();

    void removeEventType(String name);
}
