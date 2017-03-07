package org.zalando.nakadi.repository;

import java.io.Closeable;
import java.util.Optional;

import org.apache.kafka.clients.consumer.Consumer;
import org.zalando.nakadi.domain.ConsumedEvent;

public interface EventConsumer extends Closeable {

    Optional<ConsumedEvent> readEvent();

    Consumer<String, String> getConsumer();

}
