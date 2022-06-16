package org.zalando.nakadi.domain.kpi;


import org.junit.Test;
import org.zalando.nakadi.service.KPIEventMapper;

import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SubscriptionLogEventTest {
    private final KPIEventMapper eventMapper;

    public SubscriptionLogEventTest() {
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

        final var subscriptionLogGenericRecord = eventMapper
                .mapToGenericRecord(subscriptionLogEvent);

        assertEquals("created", subscriptionLogGenericRecord.get("status"));
        assertEquals(subscriptionLogEvent.getSubscriptionId(), subscriptionLogGenericRecord.get("subscription_id"));
    }
}
