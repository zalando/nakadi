package de.zalando.aruha.nakadi.repository;

import java.util.Map;
import java.util.Optional;

import de.zalando.aruha.nakadi.NakadiException;
import de.zalando.aruha.nakadi.domain.ConsumedEvent;

public interface EventConsumer {

    Optional<ConsumedEvent> readEvent() throws NakadiException;

    Map<String, String> fetchNextOffsets();

}
