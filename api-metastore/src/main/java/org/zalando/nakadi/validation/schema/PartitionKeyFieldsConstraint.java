package org.zalando.nakadi.validation.schema;

import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.EventTypeBase;
import org.zalando.nakadi.partitioning.PartitionStrategy;

import java.util.Optional;

public class  PartitionKeyFieldsConstraint implements SchemaEvolutionConstraint {
    @Override
    public Optional<SchemaEvolutionIncompatibility> validate(final EventType original, final EventTypeBase eventType) {
        if (eventType.getPartitionStrategy().equals(PartitionStrategy.HASH_STRATEGY)
                && eventType.getPartitionKeyFields().isEmpty()) {
            return Optional.of(new SchemaEvolutionIncompatibility("partition_key_fields is required " +
                    "when partition strategy is 'hash'"));
        } else {
            return Optional.empty();
        }
    }
}