package de.zalando.aruha.nakadi.enrichment;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import de.zalando.aruha.nakadi.domain.EnrichmentStrategyDescriptor;
import de.zalando.aruha.nakadi.domain.PartitionStrategyDescriptor;

import java.util.List;
import java.util.Map;

public class EnrichmentsRegistry {
    private static final Map<EnrichmentStrategyDescriptor, EnrichmentStrategy> ENRICHMENT_STRATEGIES =
        ImmutableMap.<EnrichmentStrategyDescriptor, EnrichmentStrategy>builder()
            .put(EnrichmentStrategyDescriptor.METADATA_ENRICHMENT, new MetadataEnrichmentStrategy())
            .build();

    public EnrichmentStrategy getStrategy(final EnrichmentStrategyDescriptor descriptor) {
        return ENRICHMENT_STRATEGIES.get(descriptor);
    }
}
