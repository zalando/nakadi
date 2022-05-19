package org.zalando.nakadi.validation.schema;

import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.EventTypeBase;

import java.util.Optional;

public class SchemaTypeChangeConstraint implements SchemaEvolutionConstraint{

    @Override
    public Optional<SchemaEvolutionIncompatibility> validate(final EventType original, final EventTypeBase eventType) {
        if(!original.getSchema().getType().equals(eventType.getSchema().getType())){
            return Optional.of(new SchemaEvolutionIncompatibility("changing schema_type is not allowed"));
        }

        return Optional.empty();
    }
}
