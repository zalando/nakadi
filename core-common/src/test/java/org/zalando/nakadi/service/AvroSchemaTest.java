package org.zalando.nakadi.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.avro.AvroMapper;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.springframework.core.io.DefaultResourceLoader;
import org.zalando.nakadi.config.KPIEventTypes;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class AvroSchemaTest {
    private final AvroSchema avroSchema;
    private final Map<String, List<String>> allSchemas = Map.of(
            AvroSchema.METADATA_KEY, List.of("0", "1"),
            KPIEventTypes.ACCESS_LOG, List.of("0", "1"),
            KPIEventTypes.BATCH_PUBLISHED, List.of("0"),
            KPIEventTypes.DATA_STREAMED, List.of("0"),
            KPIEventTypes.EVENT_TYPE_LOG, List.of("0"),
            KPIEventTypes.SUBSCRIPTION_LOG, List.of("0")
    );

    public AvroSchemaTest() throws IOException {
        final var eventTypeRes = new DefaultResourceLoader().getResource("event-type-schema/");
        avroSchema = new AvroSchema(new AvroMapper(), new ObjectMapper(), eventTypeRes);
    }

    @Test
    public void testAllSchemaVersionAreLoadable() {
        allSchemas.entrySet().stream()
                .flatMap(schemaEntry -> schemaEntry.getValue().stream()
                        .map(version -> Map.entry(schemaEntry.getKey(), version)))
                .forEach(schemaEntry -> Assertions.assertDoesNotThrow(
                        () -> avroSchema.getEventTypeSchema(schemaEntry.getKey(), schemaEntry.getValue()),
                        "Schema of " + schemaEntry.getKey()
                                + ", version " + schemaEntry.getValue() + " unavailable"));
    }
}
