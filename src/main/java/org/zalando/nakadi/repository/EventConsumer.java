package org.zalando.nakadi.repository;

import java.io.Closeable;
import java.util.Optional;

import org.zalando.nakadi.domain.ConsumedEvent;

public interface EventConsumer extends Closeable {

    Optional<ConsumedEvent> readEvent();

}
