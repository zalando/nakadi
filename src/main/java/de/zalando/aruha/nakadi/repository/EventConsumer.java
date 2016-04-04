package de.zalando.aruha.nakadi.repository;

import java.io.Closeable;
import java.util.Optional;

import de.zalando.aruha.nakadi.domain.ConsumedEvent;

public interface EventConsumer extends Closeable {

    Optional<ConsumedEvent> readEvent();

}
