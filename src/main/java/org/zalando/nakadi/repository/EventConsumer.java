package org.zalando.nakadi.repository;

import java.io.Closeable;
import java.util.Optional;
import java.util.Set;
import org.zalando.nakadi.domain.ConsumedEvent;
import org.zalando.nakadi.domain.TopicPartition;

public interface EventConsumer extends Closeable {

    Set<TopicPartition> getAssignment();

    Optional<ConsumedEvent> readEvent();

}
