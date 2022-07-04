package org.zalando.nakadi.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
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
        final var schema = localSchemaRegistry.getAvroSchema(BATCH_PUBLISHING_KEY, version);
        return ResponseEntity.ok(mapToVersionedAvroSchema(schema, version));
    }

    @RequestMapping(value = BATCH_CONSUMPTION_PATH, method = RequestMethod.GET)
    public ResponseEntity<?> getNakadiBatchConsumptionAvroSchemas() {
        return ResponseEntity.ok(new ItemsWrapper<>(getNakadiAvroSchemas(BATCH_CONSUMPTION_KEY)));
    }

    @RequestMapping(value = BATCH_CONSUMPTION_PATH + "/{version}", method = RequestMethod.GET)
    public ResponseEntity<?> getNakadiBatchConsumptionAvroSchemaByVersion(
            @PathVariable("version") final String version) {
        final var schema = localSchemaRegistry.getAvroSchema(BATCH_CONSUMPTION_KEY, version);
        return ResponseEntity.ok(mapToVersionedAvroSchema(schema, version));
    }

    private List<VersionedAvroSchema> getNakadiAvroSchemas(final String schemaName) {
        final var schemas = localSchemaRegistry.getAvroSchemaVersions(schemaName);
        return schemas.entrySet().stream()
                .map(entry -> mapToVersionedAvroSchema(entry.getValue(), entry.getKey()))
                .collect(Collectors.toList());
    }

    private VersionedAvroSchema mapToVersionedAvroSchema(final Schema avroSchema, final String version) {
        try {
            final JsonNode schema = objectMapper.readTree(avroSchema.toString());
            return new VersionedAvroSchema(schema, version);
        } catch (JsonProcessingException e) {
            throw new NakadiRuntimeException("Unable to map avro schema " + avroSchema.getName()
                    + ", version " + version + " to Json", e);
        }
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
