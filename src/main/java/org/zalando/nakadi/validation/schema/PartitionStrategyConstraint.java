package org.zalando.nakadi.validation.schema;

import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.EventTypeBase;
import org.zalando.nakadi.partitioning.PartitionStrategy;

import java.util.Optional;

public class PartitionStrategyConstraint implements SchemaEvolutionConstraint {
    @Override
    public Optional<SchemaEvolutionIncompatibility> validate(final EventType original, final EventTypeBase eventType) {
        if (!original.getPartitionStrategy().equals(PartitionStrategy.RANDOM_STRATEGY)
                && !eventType.getPartitionStrategy().equals(original.getPartitionStrategy())) {
            return Optional.of(new SchemaEvolutionIncompatibility("changing partition_strategy " +
                    "is only allowed if the original strategy was 'random'"));
        } else {
            return Optional.empty();
        }
    }
}