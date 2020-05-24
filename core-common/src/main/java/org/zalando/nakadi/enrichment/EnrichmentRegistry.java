package org.zalando.nakadi.enrichment;

import com.google.common.collect.ImmutableMap;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.domain.EnrichmentStrategyDescriptor;

import java.util.Map;

@Component
public class EnrichmentRegistry {

    private static final Map<EnrichmentStrategyDescriptor, EnrichmentStrategy> ENRICHMENT_STRATEGIES =
        ImmutableMap.of(EnrichmentStrategyDescriptor.METADATA_ENRICHMENT, new MetadataEnrichmentStrategy());

    public EnrichmentStrategy getStrategy(final EnrichmentStrategyDescriptor descriptor) {
        return ENRICHMENT_STRATEGIES.get(descriptor);
    }
}
