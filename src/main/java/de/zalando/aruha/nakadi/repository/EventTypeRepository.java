package de.zalando.aruha.nakadi.repository;

import de.zalando.aruha.nakadi.domain.EventType;

public interface EventTypeRepository {

    void saveEventType(EventType eventType) throws Exception;

}
