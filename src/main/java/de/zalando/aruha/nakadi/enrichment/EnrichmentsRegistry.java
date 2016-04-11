package de.zalando.aruha.nakadi.enrichment;

import com.google.common.collect.ImmutableMap;
import de.zalando.aruha.nakadi.domain.EnrichmentStrategyDescriptor;

import java.util.Map;

public class EnrichmentsRegistry {
    private static final Map<EnrichmentStrategyDescriptor, EnrichmentStrategy> ENRICHMENT_STRATEGIES =
        ImmutableMap.of(EnrichmentStrategyDescriptor.METADATA_ENRICHMENT, new MetadataEnrichmentStrategy());

    public EnrichmentStrategy getStrategy(final EnrichmentStrategyDescriptor descriptor) {
        return ENRICHMENT_STRATEGIES.get(descriptor);
    }
}
