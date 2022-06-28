package org.zalando.nakadi.controller;

import org.apache.avro.Schema;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.zalando.nakadi.domain.ItemsWrapper;
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

    @Autowired
    public NakadiAvroSchemaController(final LocalSchemaRegistry localSchemaRegistry) {
        this.localSchemaRegistry = localSchemaRegistry;
    }

    @RequestMapping(value = BATCH_PUBLISHING_PATH, method = RequestMethod.GET)
    public ResponseEntity<?> getNakadiBatchPublishingAvroSchemas() {
        return ResponseEntity.ok(new ItemsWrapper<>(getNakadiAvroSchemas(BATCH_PUBLISHING_KEY)));
    }

    @RequestMapping(value = BATCH_PUBLISHING_PATH + "/{version}", method = RequestMethod.GET)
    public ResponseEntity<?> getNakadiBatchPublishingAvroSchemaByVersion(
            @PathVariable("version") final String version) {
        return ResponseEntity.ok(localSchemaRegistry.getEventTypeSchema(BATCH_PUBLISHING_KEY, version));
    }

    @RequestMapping(value = BATCH_CONSUMPTION_PATH, method = RequestMethod.GET)
    public ResponseEntity<?> getNakadiBatchConsumptionAvroSchemas() {
        return ResponseEntity.ok(new ItemsWrapper<>(getNakadiAvroSchemas(BATCH_CONSUMPTION_KEY)));
    }

    @RequestMapping(value = BATCH_CONSUMPTION_PATH + "/{version}", method = RequestMethod.GET)
    public ResponseEntity<?> getNakadiBatchConsumptionAvroSchemaByVersion(
            @PathVariable("version") final String version) {
        return ResponseEntity.ok(localSchemaRegistry.getEventTypeSchema(BATCH_CONSUMPTION_KEY, version));
    }

    private List<VersionedAvroSchema> getNakadiAvroSchemas(final String schemaName) {
        final var schemas = localSchemaRegistry.getEventTypeSchemaVersions(schemaName);
        return schemas.entrySet().stream()
                .map(entry -> new VersionedAvroSchema(entry.getValue(), entry.getKey()))
                .collect(Collectors.toList());
    }

    public static final class VersionedAvroSchema {
        private final Schema avroSchema;
        private final String version;

        public VersionedAvroSchema(final Schema avroSchema, final String version) {
            this.avroSchema = avroSchema;
            this.version = version;
        }

        public Schema getAvroSchema() {
            return avroSchema;
        }

        public String getVersion() {
            return version;
        }
    }

}
