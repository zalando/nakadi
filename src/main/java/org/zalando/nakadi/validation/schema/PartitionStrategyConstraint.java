package org.zalando.nakadi.validation.schema;

import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.EventTypeBase;

import java.util.Optional;

public class PartitionStrategyConstraint implements SchemaEvolutionConstraint {
    @Override
    public Optional<SchemaEvolutionIncompatibility> validate(final EventType original, final EventTypeBase eventType) {
        if (eventType.getPartitionStrategy().equals(original.getPartitionStrategy())) {
            return Optional.of(new SchemaEvolutionIncompatibility("changing partition_strategy is not allowed"));
        } else {
            return Optional.empty();
        }
    }
}