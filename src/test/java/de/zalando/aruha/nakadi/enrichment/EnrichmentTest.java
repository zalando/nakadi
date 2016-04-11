package de.zalando.aruha.nakadi.enrichment;

import de.zalando.aruha.nakadi.domain.EnrichmentStrategyDescriptor;
import de.zalando.aruha.nakadi.domain.EventType;
import de.zalando.aruha.nakadi.exceptions.InvalidEventTypeException;
import org.json.JSONObject;
import org.junit.Test;
import org.mockito.Mockito;

import static de.zalando.aruha.nakadi.domain.EventCategory.BUSINESS;
import static de.zalando.aruha.nakadi.domain.EventCategory.DATA;
import static de.zalando.aruha.nakadi.utils.TestUtils.buildDefaultEventType;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

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

        final EnrichmentStrategy strategy = mock(EnrichmentStrategy.class);
        Mockito
                .doReturn(strategy)
                .when(registry)
                .getStrategy(EnrichmentStrategyDescriptor.METADATA_ENRICHMENT);

        enrichment.enrich(event, eventType);

        verify(strategy, times(1)).enrich(event, eventType);
    }
}