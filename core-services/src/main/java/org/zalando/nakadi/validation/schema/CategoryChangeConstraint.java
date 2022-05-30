package org.zalando.nakadi.validation.schema;

import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.EventTypeBase;

import java.util.Optional;

public class CategoryChangeConstraint implements SchemaEvolutionConstraint {
    @Override
    public Optional<SchemaEvolutionIncompatibility> validate(final EventType original, final EventTypeBase eventType) {
        if (!eventType.getCategory().equals(original.getCategory())) {
            return Optional.of(new SchemaEvolutionIncompatibility.CategoryIncompatibility(
                    "changing category is not allowed"));
        } else {
            return Optional.empty();
        }
    }
}
