package org.zalando.nakadi.enrichment;

import org.joda.time.DateTimeUtils;
import org.json.JSONObject;
import org.junit.Test;
import org.zalando.nakadi.domain.BatchItem;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.exceptions.runtime.EnrichmentException;
import org.zalando.nakadi.util.FlowIdUtils;
import org.zalando.nakadi.utils.TestUtils;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.isEmptyString;
import static org.junit.Assert.assertThat;
import static org.zalando.nakadi.utils.TestUtils.buildBusinessEvent;
import static org.zalando.nakadi.utils.TestUtils.buildDefaultEventType;
import static org.zalando.nakadi.utils.TestUtils.createBatchItem;
import static org.zalando.nakadi.utils.TestUtils.randomString;

public class MetadataEnrichmentStrategyTest {
    private final MetadataEnrichmentStrategy strategy = new MetadataEnrichmentStrategy();

    @Test
    public void setReceivedAtWithSystemTimeInUTC() throws Exception {
        final EventType eventType = buildDefaultEventType();
        final JSONObject event = buildBusinessEvent();
        final BatchItem batch = TestUtils.createBatchItem(event);

        assertThat(event.getJSONObject("metadata").optString("received_at"), isEmptyString());

        try {
            DateTimeUtils.setCurrentMillisFixed(0);
            strategy.enrich(batch, eventType);
        } finally {
            DateTimeUtils.setCurrentMillisSystem();
        }

        assertThat(batch.getEvent().getJSONObject("metadata").getString("received_at"),
                equalTo("1970-01-01T00:00:00.000Z"));
    }

    @Test(expected = EnrichmentException.class)
    public void throwsExceptionIfPathNotPresent() throws Exception {
        final EventType eventType = buildDefaultEventType();
        final JSONObject event = buildBusinessEvent();

        event.remove("metadata");

        strategy.enrich(TestUtils.createBatchItem(event), eventType);
    }

    @Test
    public void setEventTypeName() throws Exception {
        final EventType eventType = buildDefaultEventType();
        final JSONObject event = buildBusinessEvent();
        final BatchItem batch = TestUtils.createBatchItem(event);

        assertThat(event.getJSONObject("metadata").optString("event_type"), isEmptyString());

        strategy.enrich(batch, eventType);

        assertThat(batch.getEvent().getJSONObject("metadata").getString("event_type"), equalTo(eventType.getName()));
    }

    @Test
    public void setEventTypeSchemaVersion() throws Exception {
        final EventType eventType = buildDefaultEventType();
        final JSONObject event = buildBusinessEvent();
        final BatchItem batchItem = createBatchItem(event);

        assertThat(batchItem.getEvent().getJSONObject("metadata").optString("version"), isEmptyString());

        strategy.enrich(batchItem, eventType);

        assertThat(batchItem.getEvent().getJSONObject("metadata").getString("version"), equalTo("1.0.0"));
    }

    @Test
    public void setFlowId() throws Exception {
        final EventType eventType = buildDefaultEventType();
        final JSONObject event = buildBusinessEvent();
        final BatchItem batch = createBatchItem(event);

        assertThat(event.getJSONObject("metadata").optString("flow_id"), isEmptyString());

        final String flowId = randomString();
        FlowIdUtils.push(flowId);
        strategy.enrich(batch, eventType);

        assertThat(batch.getEvent().getJSONObject("metadata").getString("flow_id"), equalTo(flowId));
    }

    @Test
    public void whenFlowIdIsPresentDoNotOverride() throws Exception {
        final EventType eventType = buildDefaultEventType();
        final JSONObject event = buildBusinessEvent();
        event.getJSONObject("metadata").put("flow_id", "something");
        final BatchItem batch = createBatchItem(event);

        FlowIdUtils.push("something-else");
        strategy.enrich(batch, eventType);

        assertThat(batch.getEvent().getJSONObject("metadata").getString("flow_id"), equalTo("something"));
    }

    @Test
    public void whenFlowIsEmptyStringOverrideIt() throws Exception {
        final EventType eventType = buildDefaultEventType();
        final JSONObject event = buildBusinessEvent();
        event.getJSONObject("metadata").put("flow_id", "");
        final BatchItem batch = createBatchItem(event);

        final String flowId = randomString();
        FlowIdUtils.push(flowId);
        strategy.enrich(batch, eventType);

        assertThat(batch.getEvent().getJSONObject("metadata").getString("flow_id"), equalTo(flowId));
    }

    @Test
    public void whenFlowIsNullOverrideIt() throws Exception {
        final EventType eventType = buildDefaultEventType();
        final JSONObject event = buildBusinessEvent();
        event.getJSONObject("metadata").put("flow_id", (Object)null);
        final BatchItem batch = createBatchItem(event);

        final String flowId = randomString();
        FlowIdUtils.push(flowId);
        strategy.enrich(batch, eventType);

        assertThat(batch.getEvent().getJSONObject("metadata").getString("flow_id"), equalTo(flowId));
    }

    @Test
    public void setPartition() throws Exception {
        final EventType eventType = buildDefaultEventType();
        final JSONObject event = buildBusinessEvent();
        final String partition = randomString();
        final BatchItem batch = createBatchItem(event);
        batch.setPartition(partition);

        strategy.enrich(batch, eventType);

        assertThat(batch.getEvent().getJSONObject("metadata").getString("partition"), equalTo(partition));
    }
}