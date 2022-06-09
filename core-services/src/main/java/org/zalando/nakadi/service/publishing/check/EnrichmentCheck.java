package org.zalando.nakadi.service.publishing.check;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.domain.EnrichmentStrategyDescriptor;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.NakadiRecord;
import org.zalando.nakadi.domain.NakadiRecordResult;
import org.zalando.nakadi.enrichment.EnrichmentStrategy;
import org.zalando.nakadi.enrichment.EnrichmentsRegistry;
import org.zalando.nakadi.exceptions.runtime.EnrichmentException;

import java.util.Collections;
import java.util.List;

@Component
public class EnrichmentCheck extends Check {

    private final EnrichmentsRegistry registry;

    @Autowired
    public EnrichmentCheck(final EnrichmentsRegistry registry) {
        this.registry = registry;
    }

    @Override
    public List<NakadiRecordResult> execute(final EventType eventType, final List<NakadiRecord> records) {

        for (final NakadiRecord record : records) {
            try {
                enrich(record, eventType);
            } catch (EnrichmentException e) {
                return processError(records, record, e);
            }
        }

        return Collections.emptyList();
    }

    private void enrich(final NakadiRecord nakadiRecord, final EventType eventType) throws EnrichmentException {
        for (final EnrichmentStrategyDescriptor descriptor : eventType.getEnrichmentStrategies()) {
            final EnrichmentStrategy strategy = getStrategy(descriptor);
            strategy.enrich(nakadiRecord, eventType);
        }
    }

    private EnrichmentStrategy getStrategy(final EnrichmentStrategyDescriptor enrichmentStrategyDescriptor) {
        return registry.getStrategy(enrichmentStrategyDescriptor);
    }

    @Override
    public NakadiRecordResult.Step getCurrentStep() {
        return NakadiRecordResult.Step.ENRICHMENT;
    }
}
