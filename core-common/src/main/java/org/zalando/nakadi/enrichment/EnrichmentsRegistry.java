package org.zalando.nakadi.enrichment;

import com.google.common.collect.ImmutableMap;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.domain.EnrichmentStrategyDescriptor;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;

import java.util.Map;

@Component
public class EnrichmentsRegistry {

    private final Map<EnrichmentStrategyDescriptor, EnrichmentStrategy> enrichmentStrategies;

    @Autowired
    public EnrichmentsRegistry(final AuthorizationService authorizationService) {
        final MetadataEnrichmentStrategy metadataEnrichment = new MetadataEnrichmentStrategy(authorizationService);
        enrichmentStrategies = ImmutableMap.of(EnrichmentStrategyDescriptor.METADATA_ENRICHMENT, metadataEnrichment);
    }

    public EnrichmentStrategy getStrategy(final EnrichmentStrategyDescriptor descriptor) {
        return enrichmentStrategies.get(descriptor);
    }
}
