package org.zalando.nakadi.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.zalando.nakadi.domain.ItemsWrapper;
import org.zalando.nakadi.exceptions.runtime.NakadiRuntimeException;
import org.zalando.nakadi.service.LocalSchemaRegistry;

import java.util.List;
import java.util.stream.Collectors;

import static org.zalando.nakadi.service.LocalSchemaRegistry.BATCH_CONSUMPTION_KEY;
import static org.zalando.nakadi.service.LocalSchemaRegistry.BATCH_PUBLISHING_KEY;

@RestController
public class NakadiAvroSchemaController {
    private static final String BATCH_PUBLISHING_PATH = "/avro-schemas/" + BATCH_PUBLISHING_KEY + "/versions";
    private static final String BATCH_CONSUMPTION_PATH = "/avro-schemas/" + BATCH_CONSUMPTION_KEY + "/versions";
    private final LocalSchemaRegistry localSchemaRegistry;
    private final ObjectMapper objectMapper;

    @Autowired
    public NakadiAvroSchemaController(final LocalSchemaRegistry localSchemaRegistry, final ObjectMapper objectMapper) {
        this.localSchemaRegistry = localSchemaRegistry;
        this.objectMapper = objectMapper;
    }

    @RequestMapping(value = BATCH_PUBLISHING_PATH, method = RequestMethod.GET)
    public ResponseEntity<?> getNakadiBatchPublishingAvroSchemas() {
        return ResponseEntity.ok(new ItemsWrapper<>(getNakadiAvroSchemas(BATCH_PUBLISHING_KEY)));
    }

    @RequestMapping(value = BATCH_PUBLISHING_PATH + "/{version}", method = RequestMethod.GET)
    public ResponseEntity<?> getNakadiBatchPublishingAvroSchemaByVersion(
            @PathVariable("version") final String version) {
        final var schema = localSchemaRegistry.getEventTypeSchema(BATCH_PUBLISHING_KEY, version);
        return ResponseEntity.ok(schema.toString());
    }

    @RequestMapping(value = BATCH_CONSUMPTION_PATH, method = RequestMethod.GET)
    public ResponseEntity<?> getNakadiBatchConsumptionAvroSchemas() {
        return ResponseEntity.ok(new ItemsWrapper<>(getNakadiAvroSchemas(BATCH_CONSUMPTION_KEY)));
    }

    @RequestMapping(value = BATCH_CONSUMPTION_PATH + "/{version}", method = RequestMethod.GET)
    public ResponseEntity<?> getNakadiBatchConsumptionAvroSchemaByVersion(
            @PathVariable("version") final String version) {
        final var schema = localSchemaRegistry.getEventTypeSchema(BATCH_CONSUMPTION_KEY, version);
        return ResponseEntity.ok(schema.toString());
    }

    private List<VersionedAvroSchema> getNakadiAvroSchemas(final String schemaName) {
        final var schemas = localSchemaRegistry.getEventTypeSchemaVersions(schemaName);
        return schemas.entrySet().stream()
                .map(entry -> {
                    try {
                        final JsonNode schema = objectMapper.readTree(entry.getValue().toString());
                        return new VersionedAvroSchema(schema, entry.getKey());
                    } catch (JsonProcessingException e) {
                        throw new NakadiRuntimeException("Unable to map avro schema " + schemaName
                                + ", version " + entry.getKey() + " to Json", e);
                    }
                })
                .collect(Collectors.toList());
    }

    public static final class VersionedAvroSchema {
        private final JsonNode avroSchema;
        private final String version;

        public VersionedAvroSchema(final JsonNode avroSchema, final String version) {

            this.avroSchema = avroSchema;
            this.version = version;
        }

        public JsonNode getAvroSchema() {
            return avroSchema;
        }

        public String getVersion() {
            return version;
        }
    }

}
