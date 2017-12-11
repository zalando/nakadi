package org.zalando.nakadi.validation.schema;

import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.EventTypeBase;

import java.util.Optional;

public class EnrichmentStrategyConstraint implements SchemaEvolutionConstraint {
    @Override
    public Optional<SchemaEvolutionIncompatibility> validate(final EventType original, final EventTypeBase eventType) {
        if (!eventType.getEnrichmentStrategies().equals(original.getEnrichmentStrategies())) {
            return Optional.of(new SchemaEvolutionIncompatibility.MetadataIncompatibility(
                    "changing enrichment_strategies is not allowed"));
        } else {
            return Optional.empty();
        }
    }
}