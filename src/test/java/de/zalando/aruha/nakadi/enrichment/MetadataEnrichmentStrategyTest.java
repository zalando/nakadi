package de.zalando.aruha.nakadi.enrichment;

import de.zalando.aruha.nakadi.domain.BatchItem;
import de.zalando.aruha.nakadi.domain.EventType;
import de.zalando.aruha.nakadi.exceptions.EnrichmentException;
import de.zalando.aruha.nakadi.util.FlowIdUtils;
import org.joda.time.DateTimeUtils;
import org.json.JSONObject;
import org.junit.Test;

import static de.zalando.aruha.nakadi.utils.TestUtils.buildBusinessEvent;
import static de.zalando.aruha.nakadi.utils.TestUtils.buildDefaultEventType;
import static de.zalando.aruha.nakadi.utils.TestUtils.createBatch;
import static de.zalando.aruha.nakadi.utils.TestUtils.randomString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.isEmptyString;
import static org.junit.Assert.assertThat;

public class MetadataEnrichmentStrategyTest {
    private final MetadataEnrichmentStrategy strategy = new MetadataEnrichmentStrategy();

    @Test
    public void setReceivedAtWithSystemTimeInUTC() throws Exception {
        final EventType eventType = buildDefaultEventType();
        final JSONObject event = buildBusinessEvent();

        assertThat(event.getJSONObject("metadata").optString("received_at"), isEmptyString());

        try {
            DateTimeUtils.setCurrentMillisFixed(0);
            strategy.enrich(createBatch(event), eventType);
        } finally {
            DateTimeUtils.setCurrentMillisSystem();
        }

        assertThat(event.getJSONObject("metadata").getString("received_at"), equalTo("1970-01-01T00:00:00.000Z"));
    }

    @Test(expected = EnrichmentException.class)
    public void throwsExceptionIfPathNotPresent() throws Exception {
        final EventType eventType = buildDefaultEventType();
        final JSONObject event = buildBusinessEvent();

        event.remove("metadata");

        strategy.enrich(createBatch(event), eventType);
    }

    @Test
    public void setEventTypeName() throws Exception {
        final EventType eventType = buildDefaultEventType();
        final JSONObject event = buildBusinessEvent();

        assertThat(event.getJSONObject("metadata").optString("event_type"), isEmptyString());

        strategy.enrich(createBatch(event), eventType);

        assertThat(event.getJSONObject("metadata").getString("event_type"), equalTo(eventType.getName()));
    }

    @Test
    public void setFlowId() throws Exception {
        final EventType eventType = buildDefaultEventType();
        final JSONObject event = buildBusinessEvent();

        assertThat(event.getJSONObject("metadata").optString("flow_id"), isEmptyString());

        final String flowId = randomString();
        FlowIdUtils.push(flowId);
        strategy.enrich(createBatch(event), eventType);

        assertThat(event.getJSONObject("metadata").getString("flow_id"), equalTo(flowId));
    }

    @Test
    public void setPartition() throws Exception {
        final EventType eventType = buildDefaultEventType();
        final JSONObject event = buildBusinessEvent();
        final String partition = randomString();
        final BatchItem batch = createBatch(event);
        batch.setPartition(partition);

        strategy.enrich(batch, eventType);

        assertThat(event.getJSONObject("metadata").getString("partition"), equalTo(partition));
    }
}