package org.zalando.nakadi.validation.schema;

import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.EventTypeBase;

import java.util.Optional;

public class CompatibilityModeChangeConstraint implements SchemaEvolutionConstraint {
    @Override
    public Optional<SchemaEvolutionIncompatibility> validate(final EventType original, final EventTypeBase eventType) {
        if (eventType.getCompatibilityMode() != original.getCompatibilityMode()) {
            return Optional.of(new SchemaEvolutionIncompatibility("changing compatibility_mode is not allowed"));
        } else {
            return Optional.empty();
        }
    }
}
