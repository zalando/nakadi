package de.zalando.aruha.nakadi.enrichment;

import de.zalando.aruha.nakadi.domain.EnrichmentStrategyDescriptor;
import de.zalando.aruha.nakadi.domain.EventCategory;
import de.zalando.aruha.nakadi.domain.EventType;
import de.zalando.aruha.nakadi.exceptions.EnrichmentException;
import de.zalando.aruha.nakadi.exceptions.InvalidEventTypeException;
import org.json.JSONObject;

import java.util.HashSet;
import java.util.Set;

public class Enrichment {
    private final EnrichmentsRegistry registry;

    public Enrichment(final EnrichmentsRegistry registry) {
        this.registry = registry;
    }

    public void validate(final EventType eventType) throws InvalidEventTypeException {
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

    public void enrich(JSONObject event, final EventType eventType) throws EnrichmentException {
        for (EnrichmentStrategyDescriptor descriptor : eventType.getEnrichmentStrategies()) {
            EnrichmentStrategy strategy = getStrategy(descriptor);
            strategy.enrich(event, eventType);
        }
    }

    private EnrichmentStrategy getStrategy(final EnrichmentStrategyDescriptor enrichmentStrategyDescriptor) {
        return registry.getStrategy(enrichmentStrategyDescriptor);
    }
}
