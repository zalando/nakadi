package org.zalando.nakadi.domain.kpi;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.avro.AvroMapper;
import org.junit.Test;
import org.springframework.core.io.DefaultResourceLoader;
import org.zalando.nakadi.service.LocalSchemaRegistry;
import org.zalando.nakadi.service.KPIEventMapper;

import java.io.IOException;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class AccessLogEventTest {
    private final LocalSchemaRegistry localSchemaRegistry;
    private final KPIEventMapper eventMapper;

    public AccessLogEventTest() throws IOException {
        final var eventTypeRes = new DefaultResourceLoader().getResource("event-type-schema/");
        this.localSchemaRegistry = new LocalSchemaRegistry(new AvroMapper(), new ObjectMapper(), eventTypeRes);
        this.eventMapper = new KPIEventMapper(Set.of(AccessLogEvent.class));
    }

    @Test
    public void testAsJsonObject() {
        final var accessLogEvent = getRandomEvent();

        final var accessLogJson = eventMapper.mapToJsonObject(accessLogEvent);
        assertEquals(accessLogEvent.getMethod(), accessLogJson.get("method"));
        assertEquals(accessLogEvent.getPath(), accessLogJson.get("path"));
        assertEquals(accessLogEvent.getQuery(), accessLogJson.get("query"));
        assertEquals(accessLogEvent.getUserAgent(), accessLogJson.get("user_agent"));
        assertEquals(accessLogEvent.getApplicationName(), accessLogJson.get("app"));
        assertEquals(accessLogEvent.getHashedApplicationName(), accessLogJson.get("app_hashed"));
        assertEquals(accessLogEvent.getContentEncoding(), accessLogJson.get("content_encoding"));
        assertEquals(accessLogEvent.getAcceptEncoding(), accessLogJson.get("accept_encoding"));
        assertEquals(accessLogEvent.getStatusCode(), accessLogJson.get("status_code"));
        assertEquals(accessLogEvent.getTimeSpentMs(), accessLogJson.get("response_time_ms"));
        assertEquals(accessLogEvent.getRequestLength(), accessLogJson.get("request_length"));
        assertEquals(accessLogEvent.getResponseLength(), accessLogJson.get("response_length"));
    }

    @Test
    public void testAsGenericRecord() {
        final var accessLogEvent = getRandomEvent();

        final var latestSchemaEntry = localSchemaRegistry
                .getLatestEventTypeSchemaVersion(accessLogEvent.getName());
        final var accessLogGenericRecord = eventMapper
                .mapToGenericRecord(accessLogEvent, latestSchemaEntry.getSchema());

        assertEquals(accessLogEvent.getMethod(), accessLogGenericRecord.get("method"));
        assertEquals(accessLogEvent.getPath(), accessLogGenericRecord.get("path"));
        assertEquals(accessLogEvent.getQuery(), accessLogGenericRecord.get("query"));
        assertEquals(accessLogEvent.getUserAgent(), accessLogGenericRecord.get("user_agent"));
        assertEquals(accessLogEvent.getApplicationName(), accessLogGenericRecord.get("app"));
        assertEquals(accessLogEvent.getHashedApplicationName(), accessLogGenericRecord.get("app_hashed"));
        assertEquals(accessLogEvent.getContentEncoding(), accessLogGenericRecord.get("content_encoding"));
        assertEquals(accessLogEvent.getAcceptEncoding(), accessLogGenericRecord.get("accept_encoding"));
        assertEquals(accessLogEvent.getStatusCode(), accessLogGenericRecord.get("status_code"));
        assertEquals(accessLogEvent.getTimeSpentMs(), accessLogGenericRecord.get("response_time_ms"));
        assertEquals(accessLogEvent.getRequestLength(), accessLogGenericRecord.get("request_length"));
        assertEquals(accessLogEvent.getResponseLength(), accessLogGenericRecord.get("response_length"));
    }

    private AccessLogEvent getRandomEvent() {
        return new AccessLogEvent()
                .setMethod("POST")
                .setPath("/random-path")
                .setQuery("")
                .setUserAgent("random-user-agent")
                .setApplicationName("some-app")
                .setHashedApplicationName("hashed-app-name")
                .setContentEncoding("application/json")
                .setAcceptEncoding("application/json")
                .setStatusCode(200)
                .setTimeSpentMs(1234)
                .setRequestLength(1024)
                .setResponseLength(12345);
    }
}
