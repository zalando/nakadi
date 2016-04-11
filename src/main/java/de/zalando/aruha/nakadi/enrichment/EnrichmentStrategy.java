package de.zalando.aruha.nakadi.enrichment;

import de.zalando.aruha.nakadi.domain.EventType;
import de.zalando.aruha.nakadi.exceptions.EnrichmentException;
import org.json.JSONObject;

public interface EnrichmentStrategy {
    void enrich(JSONObject event, EventType eventType) throws EnrichmentException;
}
