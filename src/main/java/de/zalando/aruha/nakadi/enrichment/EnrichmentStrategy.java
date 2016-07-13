package de.zalando.aruha.nakadi.enrichment;

import de.zalando.aruha.nakadi.domain.BatchItem;
import de.zalando.aruha.nakadi.domain.EventType;
import de.zalando.aruha.nakadi.exceptions.EnrichmentException;

public interface EnrichmentStrategy {
    void enrich(BatchItem batchItem, EventType eventType) throws EnrichmentException;
}
