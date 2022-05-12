package org.zalando.nakadi.enrichment;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.avro.AvroMapper;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.joda.time.DateTimeUtils;
import org.json.JSONObject;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.core.io.DefaultResourceLoader;
import org.zalando.nakadi.domain.BatchItem;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.NakadiAvroMetadata;
import org.zalando.nakadi.domain.NakadiRecord;
import org.zalando.nakadi.exceptions.runtime.EnrichmentException;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.service.AvroSchema;
import org.zalando.nakadi.service.publishing.NakadiRecordMapper;
import org.zalando.nakadi.util.FlowIdUtils;

import java.io.IOException;
import java.util.Optional;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.isEmptyString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;
import static org.zalando.nakadi.utils.TestUtils.buildBusinessEvent;
import static org.zalando.nakadi.utils.TestUtils.buildDefaultEventType;
import static org.zalando.nakadi.utils.TestUtils.createBatchItem;
import static org.zalando.nakadi.utils.TestUtils.randomString;

public class MetadataEnrichmentStrategyTest {
    private final AuthorizationService authorizationService = Mockito.mock(AuthorizationService.class);
    private final MetadataEnrichmentStrategy strategy = new MetadataEnrichmentStrategy(authorizationService);

    private AvroSchema avroSchema;

    public MetadataEnrichmentStrategyTest() throws IOException {
        this.avroSchema = new AvroSchema(new AvroMapper(), new ObjectMapper(),
                new DefaultResourceLoader().getResource("event-type-schema/"));
    }

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
        final String partition = randomString();
        final BatchItem batch = createBatchItem(event);
        batch.setPartition(partition);

        strategy.enrich(batch, eventType);

        assertThat(batch.getEvent().getJSONObject("metadata").getString("partition"), equalTo(partition));
    }

    @Test
    public void setPublisher() throws Exception {
        when(authorizationService.getSubject()).thenReturn(Optional.of(() -> "test-user-123"));
        final EventType eventType = buildDefaultEventType();
        final JSONObject event = buildBusinessEvent();
        final String partition = randomString();
        final BatchItem batch = createBatchItem(event);
        batch.setPartition(partition);

        strategy.enrich(batch, eventType);

        assertEquals("test-user-123", batch.getEvent().getJSONObject("metadata").getString("published_by"));
    }

    @Test
    public void testNakadiRecordEnrichment() throws IOException {
        final var eventType = Mockito.mock(EventType.class);
        final var nakadiRecord = getTestNakadiRecord();

        strategy.enrich(nakadiRecord, eventType);

        final var metadata = nakadiRecord.getMetadata();
        assertEquals("unauthenticated", metadata.getPublishedBy());
        assertTrue(metadata.getReceivedAt() > metadata.getOccurredAt());
    }

    private NakadiRecord getTestNakadiRecord() throws IOException {

        final long now = System.currentTimeMillis();
        final var nakadiAccessLog = "nakadi.access.log";
        final var latestMeta = avroSchema.getLatestEventTypeSchemaVersion(AvroSchema.METADATA_KEY);
        final var latestSchema = avroSchema.getLatestEventTypeSchemaVersion(nakadiAccessLog);

        final var nakadiAvroMetadata = new NakadiAvroMetadata(
                Byte.parseByte(latestMeta.getVersion()), latestMeta.getSchema());
        nakadiAvroMetadata.setOccurredAt(now);
        nakadiAvroMetadata.setEid(UUID.randomUUID().toString());
        nakadiAvroMetadata.setFlowId("test-flow");
        nakadiAvroMetadata.setEventType("nakadi.test-2022-05-06.et");
        nakadiAvroMetadata.setSchemaVersion(latestSchema.getVersion());
        nakadiAvroMetadata.setPublishedBy("test-user");


        final GenericRecord event = new GenericRecordBuilder(latestSchema.getSchema())
                .set("method", "POST")
                .set("path", "/event-types")
                .set("query", "")
                .set("user_agent", "test-user-agent")
                .set("app", "nakadi")
                .set("app_hashed", "hashed-app")
                .set("status_code", 201)
                .set("response_time_ms", 10)
                .set("accept_encoding", "-")
                .set("content_encoding", "--")
                .set("request_length", 123)
                .set("response_length", 321)
                .build();

        return new NakadiRecordMapper(avroSchema)
                .fromAvroGenericRecord(nakadiAvroMetadata, event);

    }
}
