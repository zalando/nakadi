package org.zalando.nakadi.validation.schema;

import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.EventTypeBase;

import java.util.Optional;

public class CompatibleSchemaChangeConstraint implements SchemaEvolutionConstraint {
    // TODO: to be done next
    @Override
    public Optional<SchemaEvolutionIncompatibility> validate(final EventType original, final EventTypeBase eventType) {
//        if (original.getCompatibilityMode() == CompatibilityMode.COMPATIBLE) {
//            if(!original.getSchema().getSchema().equals(eventType.getSchema().getSchema())) {
//                return Optional.of(new SchemaEvolutionIncompatibility("not yet implemented schema changes"));
//            }
//        }
        return Optional.empty();
    }
}
