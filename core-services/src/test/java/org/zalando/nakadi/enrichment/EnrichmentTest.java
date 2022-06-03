package org.zalando.nakadi.enrichment;

import org.json.JSONObject;
import org.junit.Test;
import org.mockito.Mockito;
import org.zalando.nakadi.domain.BatchItem;
import org.zalando.nakadi.domain.EnrichmentStrategyDescriptor;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.exceptions.runtime.InvalidEventTypeException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.zalando.nakadi.domain.EventCategory.BUSINESS;
import static org.zalando.nakadi.domain.EventCategory.DATA;
import static org.zalando.nakadi.utils.TestUtils.buildDefaultEventType;
import static org.zalando.nakadi.utils.TestUtils.createBatchItem;

public class EnrichmentTest {
    private final EnrichmentsRegistry registry = mock(EnrichmentsRegistry.class);
    private final Enrichment enrichment = new Enrichment(registry);

    @Test(expected = InvalidEventTypeException.class)
    public void whenEventTypeCategoryUndefinedThenEnrichmentShouldBeEmpty() throws Exception {
        final EventType eventType = buildDefaultEventType();
        eventType.getEnrichmentStrategies().add(EnrichmentStrategyDescriptor.METADATA_ENRICHMENT);

        enrichment.validate(eventType);
    }

    @Test(expected = InvalidEventTypeException.class)
    public void businessEventsShouldDefineMetadataEnrichment() throws Exception {
        final EventType eventType = buildDefaultEventType();
        eventType.setCategory(BUSINESS);

        enrichment.validate(eventType);
    }

    @Test(expected = InvalidEventTypeException.class)
    public void dataEventsShouldDefineMetadataEnrichment() throws Exception {
        final EventType eventType = buildDefaultEventType();
        eventType.setCategory(DATA);

        enrichment.validate(eventType);
    }

    @Test(expected = InvalidEventTypeException.class)
    public void validateEnrichmentStrategyUniqueness() throws Exception {
        final EventType eventType = buildDefaultEventType();
        eventType.setCategory(BUSINESS);
        eventType.getEnrichmentStrategies().add(EnrichmentStrategyDescriptor.METADATA_ENRICHMENT);
        eventType.getEnrichmentStrategies().add(EnrichmentStrategyDescriptor.METADATA_ENRICHMENT);

        enrichment.validate(eventType);
    }

    @Test
    public void enrichAppliesStrategies() throws Exception {
        final EventType eventType = buildDefaultEventType();
        eventType.getEnrichmentStrategies().add(EnrichmentStrategyDescriptor.METADATA_ENRICHMENT);
        final JSONObject event = new JSONObject();
        final BatchItem batchItem = createBatchItem(event);

        final EnrichmentStrategy strategy = mock(EnrichmentStrategy.class);
        Mockito
                .doReturn(strategy)
                .when(registry)
                .getStrategy(EnrichmentStrategyDescriptor.METADATA_ENRICHMENT);

        enrichment.enrich(batchItem, eventType);

        verify(strategy, times(1)).enrich(batchItem, eventType);
    }
}
