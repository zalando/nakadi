package org.zalando.nakadi.enrichment;

import org.joda.time.DateTimeUtils;
import org.json.JSONObject;
import org.junit.Test;
import org.mockito.Mockito;
import org.zalando.nakadi.domain.BatchItem;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.exceptions.runtime.EnrichmentException;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.util.FlowIdUtils;

import java.util.Optional;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.isEmptyString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;
import static org.zalando.nakadi.utils.TestUtils.buildBusinessEvent;
import static org.zalando.nakadi.utils.TestUtils.buildDefaultEventType;
import static org.zalando.nakadi.utils.TestUtils.createBatchItem;
import static org.zalando.nakadi.utils.TestUtils.randomString;
import static org.zalando.nakadi.utils.TestUtils.randomUInt;

public class MetadataEnrichmentStrategyTest {
    private final AuthorizationService authorizationService = Mockito.mock(AuthorizationService.class);
    private final MetadataEnrichmentStrategy strategy = new MetadataEnrichmentStrategy(authorizationService);

    @Test
    public void setReceivedAtWithSystemTimeInUTC() throws Exception {
        final EventType eventType = buildDefaultEventType();
        final JSONObject event = buildBusinessEvent();
        final BatchItem batch = createBatchItem(event);

        assertThat(event.getJSONObject("metadata").optString("received_at"), isEmptyString());

        try {
            DateTimeUtils.setCurrentMillisFixed(0);
            strategy.enrich(batch, eventType);
        } finally {
            DateTimeUtils.setCurrentMillisSystem();
        }

        assertThat(batch.getEvent()
                        .getJSONObject("metadata")
                        .getString("received_at"),
                equalTo("1970-01-01T00:00:00.000Z"));
    }

    @Test(expected = EnrichmentException.class)
    public void throwsExceptionIfPathNotPresent() throws Exception {
        final EventType eventType = buildDefaultEventType();
        final JSONObject event = buildBusinessEvent();

        event.remove("metadata");

        strategy.enrich(createBatchItem(event), eventType);
    }

    @Test
    public void setEventTypeName() throws Exception {
        final EventType eventType = buildDefaultEventType();
        final JSONObject event = buildBusinessEvent();
        final BatchItem batch = createBatchItem(event);

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
        event.getJSONObject("metadata").put("flow_id", (Object) null);
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
        final Integer partition = randomUInt();
        final BatchItem item = createBatchItem(event);
        item.setPartition(partition);

        strategy.enrich(item, eventType);

        final String metaPartition = item.getEvent().getJSONObject("metadata").getString("partition");
        assertThat(Integer.valueOf(metaPartition), equalTo(partition));
    }

    @Test
    public void setPublisher() throws Exception {
        when(authorizationService.getSubject()).thenReturn(Optional.of(() -> "test-user-123"));
        final EventType eventType = buildDefaultEventType();
        final JSONObject event = buildBusinessEvent();
        final Integer partition = randomUInt();
        final BatchItem item = createBatchItem(event);
        item.setPartition(partition);

        strategy.enrich(item, eventType);

        assertEquals("test-user-123", item.getEvent().getJSONObject("metadata").getString("published_by"));
    }

}
