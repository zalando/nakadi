package de.zalando.aruha.nakadi.repository;

import java.util.Optional;

import de.zalando.aruha.nakadi.domain.ConsumedEvent;

public interface EventConsumer {

    Optional<ConsumedEvent> readEvent();

}
