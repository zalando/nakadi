package org.zalando.nakadi.enrichment;

import org.zalando.nakadi.domain.BatchItem;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.NakadiRecord;
import org.zalando.nakadi.exceptions.runtime.EnrichmentException;

public interface EnrichmentStrategy {
    void enrich(BatchItem batchItem, EventType eventType) throws EnrichmentException;

    void enrich(NakadiRecord nakadiRecord, EventType eventType) throws EnrichmentException;
}
