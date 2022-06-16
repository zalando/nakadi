package org.zalando.nakadi.domain.kpi;

import org.junit.Test;
import org.zalando.nakadi.service.KPIEventMapper;

import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class EventTypeLogEventTest {
    private final KPIEventMapper eventMapper;

    public EventTypeLogEventTest() {
        this.eventMapper = new KPIEventMapper(Set.of(EventTypeLogEvent.class));
    }

    @Test
    public void testAsJsonObject() {
        final var eventTypeLogEvent = getRandomEventTypeLogEvent();

        final var eventTypeLogJson = eventMapper.mapToJsonObject(eventTypeLogEvent);

        System.out.println(eventTypeLogJson.toString());
        assertEquals(eventTypeLogEvent.getEventType(), eventTypeLogJson.get("event_type"));
        assertEquals(eventTypeLogEvent.getStatus(), eventTypeLogJson.get("status"));
        assertEquals(eventTypeLogEvent.getCompatibilityMode(), eventTypeLogJson.get("compatibility_mode"));
        assertEquals(eventTypeLogEvent.getAuthz(), eventTypeLogJson.get("authz"));
    }

    @Test
    public void testAsGenericRecord() {
        final var eventTypeLogEvent = getRandomEventTypeLogEvent();

        final var eventTypeLogGenericRecord = eventMapper
                .mapToGenericRecord(eventTypeLogEvent);

        assertEquals(eventTypeLogEvent.getEventType(), eventTypeLogGenericRecord.get("event_type"));
        assertEquals(eventTypeLogEvent.getStatus(), eventTypeLogGenericRecord.get("status"));
        assertEquals(eventTypeLogEvent.getCompatibilityMode(), eventTypeLogGenericRecord.get("compatibility_mode"));
        assertEquals(eventTypeLogEvent.getAuthz(), eventTypeLogGenericRecord.get("authz"));
    }

    private EventTypeLogEvent getRandomEventTypeLogEvent() {
        return new EventTypeLogEvent()
                .setEventType("test-et-" + UUID.randomUUID())
                .setStatus("status-created")
                .setCompatibilityMode("forward")
                .setCategory("business")
                .setAuthz("disabled");
    }
}
