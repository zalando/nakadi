package org.zalando.nakadi.domain.kpi;

import org.junit.Test;
import org.zalando.nakadi.service.KPIEventMapper;

import java.util.Set;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class DataStreamedEventTest {
    private final KPIEventMapper eventMapper;

    public DataStreamedEventTest() {
        this.eventMapper = new KPIEventMapper(Set.of(DataStreamedEvent.class));
    }

    @Test
    public void testAsJsonObject() {
        final var dataStreamedEvent = getRandomEvent()
                .setApi("hila")
                .setSubscriptionId(UUID.randomUUID().toString());

        final var dataStreamedJsonObject = eventMapper.mapToJsonObject(dataStreamedEvent);

        assertEquals(dataStreamedEvent.getEventTypeName(), dataStreamedJsonObject.get("event_type"));
        assertEquals(dataStreamedEvent.getApplicationName(), dataStreamedJsonObject.get("app"));
        assertEquals(dataStreamedEvent.getHashedApplicationName(), dataStreamedJsonObject.get("app_hashed"));
        assertEquals(dataStreamedEvent.getTokenRealm(), dataStreamedJsonObject.get("token_realm"));
        assertEquals(dataStreamedEvent.getNumberOfEvents(), dataStreamedJsonObject.get("number_of_events"));
        assertEquals(dataStreamedEvent.getBatchesStreamed(), dataStreamedJsonObject.get("batches_streamed"));
        assertEquals(dataStreamedEvent.getBytesStreamed(), dataStreamedJsonObject.get("bytes_streamed"));
        assertEquals(dataStreamedEvent.getApi(), dataStreamedJsonObject.get("api"));
        assertEquals(dataStreamedEvent.getSubscriptionId(), dataStreamedJsonObject.get("subscription"));
    }

    @Test
    public void testAsGenericRecord() {
        final var dataStreamedEvent = getRandomEvent()
                .setApi("lola");

        final var dataStreamedGenericRecord = eventMapper
                .mapToGenericRecord(dataStreamedEvent);

        assertEquals(dataStreamedEvent.getEventTypeName(), dataStreamedGenericRecord.get("event_type"));
        assertEquals(dataStreamedEvent.getApplicationName(), dataStreamedGenericRecord.get("app"));
        assertEquals(dataStreamedEvent.getHashedApplicationName(), dataStreamedGenericRecord.get("app_hashed"));
        assertEquals(dataStreamedEvent.getTokenRealm(), dataStreamedGenericRecord.get("token_realm"));
        assertEquals(dataStreamedEvent.getNumberOfEvents(), dataStreamedGenericRecord.get("number_of_events"));
        assertEquals(dataStreamedEvent.getBatchesStreamed(), dataStreamedGenericRecord.get("batches_streamed"));
        assertEquals(dataStreamedEvent.getBytesStreamed(), dataStreamedGenericRecord.get("bytes_streamed"));
        assertEquals(dataStreamedEvent.getApi(), dataStreamedGenericRecord.get("api"));
        assertNull(dataStreamedGenericRecord.get("subscription"));

        dataStreamedEvent.setApi("hila")
                .setSubscriptionId(UUID.randomUUID().toString());
        final var dataStreamedGenericRecord2 = eventMapper
                .mapToGenericRecord(dataStreamedEvent);
        assertEquals(dataStreamedEvent.getApi(), dataStreamedGenericRecord2.get("api"));
        assertEquals(dataStreamedEvent.getSubscriptionId(), dataStreamedGenericRecord2.get("subscription"));


    }

    private DataStreamedEvent getRandomEvent() {
        return new DataStreamedEvent()
                .setEventTypeName("et-" + UUID.randomUUID())
                .setApplicationName("test-app")
                .setHashedApplicationName("hashed-name")
                .setTokenRealm("test-realm")
                .setNumberOfEvents(123)
                .setBytesStreamed(12345)
                .setBatchesStreamed(12);
    }
}
