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

public class SubscriptionLogEventTest {
    private final AvroSchema avroSchema;
    private final KPIEventMapper eventMapper;

    public SubscriptionLogEventTest() throws IOException {
        final var eventTypeRes = new DefaultResourceLoader().getResource("event-type-schema/");
        this.avroSchema = new AvroSchema(new AvroMapper(), new ObjectMapper(), eventTypeRes);
        this.eventMapper = new KPIEventMapper(Set.of(SubscriptionLogEvent.class));
    }

    @Test
    public void testAsJsonObject() {
        final var subscriptionLogEvent = new SubscriptionLogEvent()
                .setSubscriptionId(UUID.randomUUID().toString())
                .setStatus("created");

        final var subscriptionLogJsonObject = eventMapper.mapToJsonObject(subscriptionLogEvent);

        System.out.println(subscriptionLogJsonObject.toString());
        assertEquals("created", subscriptionLogJsonObject.get("status"));
        assertEquals(subscriptionLogEvent.getSubscriptionId(), subscriptionLogJsonObject.get("subscription_id"));
    }

    @Test
    public void testAsGenericRecord() {
        final var subscriptionLogEvent = new SubscriptionLogEvent()
                .setSubscriptionId(UUID.randomUUID().toString())
                .setStatus("created");

        final var latestSchemaEntry = avroSchema
                .getLatestEventTypeSchemaVersion(subscriptionLogEvent.eventTypeOfThisKPIEvent());

        final var subscriptionLogGenericRecord = eventMapper
                .mapToGenericRecord(subscriptionLogEvent, latestSchemaEntry.getSchema());

        assertEquals("created", subscriptionLogGenericRecord.get("status"));
        assertEquals(subscriptionLogEvent.getSubscriptionId(), subscriptionLogGenericRecord.get("subscription_id"));
    }
}
