package org.zalando.nakadi.validation.schema;

import org.zalando.nakadi.domain.EventType;

import java.util.Optional;

public class ChangeVersionAndCreatedAtConstraint implements SchemaEvolutionConstraint {

    @Override
    public Optional<SchemaEvolutionIncompatibility> validate(final EventType original, final EventType eventType) {
        if (original.getSchema().equals(eventType.getSchema())) {
            if (!original.getSchema().getVersion().equals(eventType.getSchema().getVersion())) {
                return Optional.of(new SchemaEvolutionIncompatibility("changing schema version is not allowed"));
            } else if (!original.getSchema().getCreatedAt().equals(eventType.getSchema().getCreatedAt())) {
                return Optional.of(new SchemaEvolutionIncompatibility("changing schema created_at is not allowed"));
            }
        }
        return Optional.empty();
    }
}
