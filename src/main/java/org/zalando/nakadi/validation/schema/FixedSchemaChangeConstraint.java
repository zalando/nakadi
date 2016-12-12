package org.zalando.nakadi.validation.schema;

import org.zalando.nakadi.domain.CompatibilityMode;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.EventTypeBase;

import java.util.Optional;

public class FixedSchemaChangeConstraint implements SchemaEvolutionConstraint {
    private final String errorMessage = "schema from an event types with fixed compatibility mode cannot be " +
            "changed. Please, upgrade this event type to compatible mode in order to be able to change the schema.";
    @Override
    public Optional<SchemaEvolutionIncompatibility> validate(final EventType original, final EventTypeBase eventType) {
        if (original.getCompatibilityMode() == CompatibilityMode.FIXED
            && eventType.getCompatibilityMode() == CompatibilityMode.FIXED) {
            if (!original.getSchema().getSchema().equals(eventType.getSchema().getSchema())) {
                return Optional.of(new SchemaEvolutionIncompatibility(errorMessage));
            }
        }
        return Optional.empty();
    }
}
