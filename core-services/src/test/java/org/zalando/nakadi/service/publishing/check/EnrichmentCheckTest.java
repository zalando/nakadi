package org.zalando.nakadi.service.publishing.check;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.zalando.nakadi.domain.EnrichmentStrategyDescriptor;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.NakadiMetadata;
import org.zalando.nakadi.domain.NakadiRecord;
import org.zalando.nakadi.domain.NakadiRecordResult;
import org.zalando.nakadi.enrichment.EnrichmentsRegistry;
import org.zalando.nakadi.enrichment.MetadataEnrichmentStrategy;
import org.zalando.nakadi.exceptions.runtime.EnrichmentException;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(MockitoJUnitRunner.class)
public class EnrichmentCheckTest {

    @Mock
    private MetadataEnrichmentStrategy metadataEnrichmentStrategy;
    @Mock
    private EnrichmentsRegistry enrichmentsRegistry;
    private EnrichmentCheck enrichmentCheck;

    @Before
    public void init() {
        this.enrichmentCheck = new EnrichmentCheck(enrichmentsRegistry);
        Mockito.when(enrichmentsRegistry.getStrategy(EnrichmentStrategyDescriptor.METADATA_ENRICHMENT))
                .thenReturn(metadataEnrichmentStrategy);
    }

    @Test
    public void testExecuteSuccess() {
        final EventType eventType = Mockito.mock(EventType.class);
        Mockito.when(eventType.getEnrichmentStrategies())
                .thenReturn(List.of(EnrichmentStrategyDescriptor.METADATA_ENRICHMENT));
        final NakadiRecord nakadiRecord = Mockito.mock(NakadiRecord.class);
        final List<NakadiRecord> nakadiRecords = List.of(nakadiRecord);

        final List<NakadiRecordResult> result = enrichmentCheck.execute(eventType, nakadiRecords);
        assertNotNull(result);
        assertEquals(0, result.size());

        Mockito.verify(metadataEnrichmentStrategy).enrich(nakadiRecord, eventType);
    }

    @Test
    public void testExecuteError() {
        final EventType eventType = Mockito.mock(EventType.class);
        Mockito.when(eventType.getEnrichmentStrategies())
                .thenReturn(List.of(EnrichmentStrategyDescriptor.METADATA_ENRICHMENT));
        final NakadiRecord nakadiRecord = Mockito.mock(NakadiRecord.class);
        final List<NakadiRecord> nakadiRecords = List.of(nakadiRecord);
        final NakadiMetadata metadata = Mockito.mock(NakadiMetadata.class);
        Mockito.when(nakadiRecord.getMetadata()).thenReturn(metadata);
        Mockito.doThrow(EnrichmentException.class).when(metadataEnrichmentStrategy).enrich(nakadiRecord, eventType);
        final List<NakadiRecordResult> recordResults = enrichmentCheck.execute(eventType, nakadiRecords);
        assertNotNull(recordResults);
        assertEquals(1, recordResults.size());
    }

    @Test
    public void testGetCurrentStep() {
        assertEquals(NakadiRecordResult.Step.ENRICHMENT, enrichmentCheck.getCurrentStep());
    }
}