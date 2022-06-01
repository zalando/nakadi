package org.zalando.nakadi.enrichment;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.domain.BatchItem;
import org.zalando.nakadi.domain.EnrichmentStrategyDescriptor;
import org.zalando.nakadi.domain.EventCategory;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.EventTypeBase;
import org.zalando.nakadi.exceptions.runtime.EnrichmentException;
import org.zalando.nakadi.exceptions.runtime.InvalidEventTypeException;

import java.util.HashSet;
import java.util.Set;

@Component
public class Enrichment {

    private final EnrichmentsRegistry registry;

    @Autowired
    public Enrichment(final EnrichmentsRegistry registry) {
        this.registry = registry;
    }

    public void validate(final EventTypeBase eventType) throws InvalidEventTypeException {
        if (eventType.getCategory() == EventCategory.UNDEFINED && !eventType.getEnrichmentStrategies().isEmpty()) {
            throw new InvalidEventTypeException("must not define enrichment strategy for undefined event type");
        }

        final Set<EnrichmentStrategyDescriptor> uniqueStrategies = new HashSet<>(eventType.getEnrichmentStrategies());
        if (eventType.getEnrichmentStrategies().size() != uniqueStrategies.size()) {
            throw new InvalidEventTypeException("enrichment strategies must not contain duplicated entries");
        }

        if (eventType.getCategory() != EventCategory.UNDEFINED
                && !eventType.getEnrichmentStrategies().contains(EnrichmentStrategyDescriptor.METADATA_ENRICHMENT)) {
            throw new InvalidEventTypeException("must define metadata enrichment strategy");
        }
    }

    public void enrich(final BatchItem batchItem, final EventType eventType)
            throws EnrichmentException {

        for (final EnrichmentStrategyDescriptor descriptor : eventType.getEnrichmentStrategies()) {
            final EnrichmentStrategy strategy = getStrategy(descriptor);
            strategy.enrich(batchItem, eventType);
        }
    }

    private EnrichmentStrategy getStrategy(final EnrichmentStrategyDescriptor enrichmentStrategyDescriptor) {
        return registry.getStrategy(enrichmentStrategyDescriptor);
    }
}
