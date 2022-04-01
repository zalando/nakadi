package org.zalando.nakadi.domain.kpi;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.avro.AvroMapper;
import org.junit.Test;
import org.springframework.core.io.DefaultResourceLoader;
import org.zalando.nakadi.service.AvroSchema;

import java.io.IOException;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SubscriptionLogEventTest {
    private final AvroSchema avroSchema;

    public SubscriptionLogEventTest() throws IOException {
        final var eventTypeRes = new DefaultResourceLoader().getResource("event-type-schema/");
        this.avroSchema = new AvroSchema(new AvroMapper(), new ObjectMapper(), eventTypeRes);
    }

    @Test
    public void testAsJsonObject() {
        final var subscriptionLogEvent = new SubscriptionLogEvent()
                .setSubscriptionId(UUID.randomUUID().toString())
                .setStatus("created");

        final var subscriptionLogJsonObject = subscriptionLogEvent.asJsonObject();

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

        final var subscriptionLogGenericRecord = subscriptionLogEvent
                .asGenericRecord(latestSchemaEntry.getValue());

        assertEquals("created", subscriptionLogGenericRecord.get("status"));
        assertEquals(subscriptionLogEvent.getSubscriptionId(), subscriptionLogGenericRecord.get("subscription_id"));
    }
}
