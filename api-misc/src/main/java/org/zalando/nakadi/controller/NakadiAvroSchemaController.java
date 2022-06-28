package org.zalando.nakadi.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.zalando.nakadi.domain.ItemsWrapper;
import org.zalando.nakadi.service.NakadiAvroSchemaService;

@RestController
@RequestMapping(value = "/nakadi/avro-schemas")
public class NakadiAvroSchemaController {
    private static final String BATCH_CONSUMPTION = "batch.consumption";
    private static final String BATCH_PUBLISHING = "batch.publishing";
    private final NakadiAvroSchemaService nakadiAvroSchemaService;

    @Autowired
    public NakadiAvroSchemaController(final NakadiAvroSchemaService nakadiAvroSchemaService) {
        this.nakadiAvroSchemaService = nakadiAvroSchemaService;
    }

    @RequestMapping(value = "batch.publishing", method = RequestMethod.GET)
    public ResponseEntity<?> getNakadiBatchPublishingAvroSchemas() {
        return ResponseEntity.ok(new ItemsWrapper<>(nakadiAvroSchemaService
                .getNakadiAvroSchemas(BATCH_PUBLISHING)));
    }

    @RequestMapping(value = "batch.publishing/versions/{version}", method = RequestMethod.GET)
    public ResponseEntity<?> getNakadiBatchPublishingAvroSchemasFiltered(
            @PathVariable("version") final String version) {
        return ResponseEntity.ok(new ItemsWrapper<>(nakadiAvroSchemaService
                .getNakadiAvroSchemas(BATCH_PUBLISHING, version)));
    }

    @RequestMapping(value = "batch.consumption", method = RequestMethod.GET)
    public ResponseEntity<?> getNakadiBatchConsumptionAvroSchemas() {
        return ResponseEntity.ok(new ItemsWrapper<>(nakadiAvroSchemaService
                .getNakadiAvroSchemas(BATCH_CONSUMPTION)));
    }

    @RequestMapping(value = "batch.consumption/versions/{version}", method = RequestMethod.GET)
    public ResponseEntity<?> getNakadiBatchConsumptionAvroSchemasFiltered(
            @PathVariable("version") final String version) {
        return ResponseEntity.ok(new ItemsWrapper<>(nakadiAvroSchemaService
                .getNakadiAvroSchemas(BATCH_CONSUMPTION, version)));
    }
}
