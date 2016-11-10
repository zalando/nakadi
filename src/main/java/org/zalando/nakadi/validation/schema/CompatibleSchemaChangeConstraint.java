package org.zalando.nakadi.validation.schema;

import org.zalando.nakadi.domain.CompatibilityMode;
import org.zalando.nakadi.domain.EventType;

import java.util.Optional;

public class CompatibleSchemaChangeConstraint implements SchemaEvolutionConstraint {
    // TODO: to be done next
    @Override
    public Optional<SchemaEvolutionIncompatibility> validate(final EventType original, final EventType eventType) {
        if (original.getCompatibilityMode() == CompatibilityMode.COMPATIBLE) {
            return Optional.empty();
        }
        return Optional.empty();
    }
}
