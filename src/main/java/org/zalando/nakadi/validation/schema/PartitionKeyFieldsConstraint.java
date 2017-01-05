package org.zalando.nakadi.validation.schema;

import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.EventTypeBase;

import java.util.Optional;

public class PartitionKeyFieldsConstraint implements SchemaEvolutionConstraint {
    @Override
    public Optional<SchemaEvolutionIncompatibility> validate(final EventType original, final EventTypeBase eventType) {
        if (!eventType.getPartitionKeyFields().equals(original.getPartitionKeyFields())) {
            return Optional.of(new SchemaEvolutionIncompatibility("changing partition_key_fields is not allowed"));
        } else {
            return Optional.empty();
        }
    }
}