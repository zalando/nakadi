package org.zalando.nakadi.validation.schema;

import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.EventTypeBase;

import java.util.Optional;

public interface SchemaEvolutionConstraint {
    Optional<SchemaEvolutionIncompatibility> validate(EventType original, EventTypeBase eventType);
}
