package de.zalando.aruha.nakadi.repository;

import de.zalando.aruha.nakadi.domain.ConsumedEvent;

import java.util.Map;
import java.util.Optional;

public interface EventConsumer {

    Optional<ConsumedEvent> readEvent();

    Map<String, String> fetchNextOffsets();

}
