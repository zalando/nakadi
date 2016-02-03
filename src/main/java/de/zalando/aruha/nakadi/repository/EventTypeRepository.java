package de.zalando.aruha.nakadi.repository;

import de.zalando.aruha.nakadi.NakadiException;
import de.zalando.aruha.nakadi.domain.EventType;
import org.springframework.dao.DuplicateKeyException;

import java.io.IOException;
import java.util.Optional;

public interface EventTypeRepository {

    void saveEventType(EventType eventType) throws NakadiException, DuplicatedEventTypeNameException;

    Optional<EventType> findByName(String name) throws NakadiException;

}
