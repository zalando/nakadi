package org.zalando.nakadi.domain.kpi;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.avro.AvroMapper;
import org.junit.Test;
import org.springframework.core.io.DefaultResourceLoader;
import org.zalando.nakadi.service.AvroSchema;
import org.zalando.nakadi.service.KPIEventMapper;

import java.io.IOException;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class BatchPublishedEventTest {
    private final AvroSchema avroSchema;
    private final KPIEventMapper eventMapper;

    public BatchPublishedEventTest() throws IOException {
        final var eventTypeRes = new DefaultResourceLoader().getResource("event-type-schema/");
        this.avroSchema = new AvroSchema(new AvroMapper(), new ObjectMapper(), eventTypeRes);
        this.eventMapper = new KPIEventMapper(Set.of(BatchPublishedEvent.class));
    }

    @Test
    public void testAsJsonObject() {
        final var eventTypeLogEvent = getRandomBatchPublishedEventObject();

        final var eventTypeLogJson = eventMapper.mapToJsonObject(eventTypeLogEvent);

        System.out.println(eventTypeLogJson.toString());
        assertEquals(eventTypeLogEvent.getEventTypeName(), eventTypeLogJson.get("event_type"));
        assertEquals(eventTypeLogEvent.getApplicationName(), eventTypeLogJson.get("app"));
        assertEquals(eventTypeLogEvent.getHashedApplicationName(), eventTypeLogJson.get("app_hashed"));
        assertEquals(eventTypeLogEvent.getTokenRealm(), eventTypeLogJson.get("token_realm"));
        assertEquals(eventTypeLogEvent.getEventCount(), eventTypeLogJson.get("number_of_events"));
        assertEquals(eventTypeLogEvent.getMsSpent(), eventTypeLogJson.get("ms_spent"));
        assertEquals(eventTypeLogEvent.getTotalSizeBytes(), eventTypeLogJson.get("batch_size"));
    }

    @Test
    public void testAsGenericRecord() {
        final var eventTypeLogEvent = getRandomBatchPublishedEventObject();

        final var latestSchemaEntry = avroSchema
                .getLatestEventTypeSchemaVersion(eventTypeLogEvent.eventTypeOfThisKPIEvent());

        final var eventTypeLogGenericRecord = eventMapper
                .mapToGenericRecord(eventTypeLogEvent, latestSchemaEntry.getSchema());

        assertEquals(eventTypeLogEvent.getEventTypeName(), eventTypeLogGenericRecord.get("event_type"));
        assertEquals(eventTypeLogEvent.getApplicationName(), eventTypeLogGenericRecord.get("app"));
        assertEquals(eventTypeLogEvent.getHashedApplicationName(), eventTypeLogGenericRecord.get("app_hashed"));
        assertEquals(eventTypeLogEvent.getTokenRealm(), eventTypeLogGenericRecord.get("token_realm"));
        assertEquals(eventTypeLogEvent.getEventCount(), eventTypeLogGenericRecord.get("number_of_events"));
        assertEquals(eventTypeLogEvent.getMsSpent(), eventTypeLogGenericRecord.get("ms_spent"));
        assertEquals(eventTypeLogEvent.getTotalSizeBytes(), eventTypeLogGenericRecord.get("batch_size"));
    }

    private BatchPublishedEvent getRandomBatchPublishedEventObject() {
        return new BatchPublishedEvent()
                .setEventTypeName("et-" + UUID.randomUUID())
                .setApplicationName("app-" + UUID.randomUUID())
                .setHashedApplicationName("app-hashed-" + UUID.randomUUID())
                .setTokenRealm("test-realm")
                .setEventCount(33)
                .setMsSpent(123L)
                .setTotalSizeBytes(1234);

    }
}
