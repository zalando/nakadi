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
public class NakadiAvroSchemaController {
    private static final String BATCH_PUBLISHING = "batch.publishing";
    private static final String BATCH_PUBLISHING_PATH = "/avro-schemas/" + BATCH_PUBLISHING + "/versions";
    private static final String BATCH_CONSUMPTION = "batch.consumption";
    private static final String BATCH_CONSUMPTION_PATH = "/avro-schemas/" + BATCH_CONSUMPTION + "/versions";
    private final NakadiAvroSchemaService nakadiAvroSchemaService;

    @Autowired
    public NakadiAvroSchemaController(final NakadiAvroSchemaService nakadiAvroSchemaService) {
        this.nakadiAvroSchemaService = nakadiAvroSchemaService;
    }

    @RequestMapping(value = BATCH_PUBLISHING_PATH, method = RequestMethod.GET)
    public ResponseEntity<?> getNakadiBatchPublishingAvroSchemas() {
        return ResponseEntity.ok(new ItemsWrapper<>(nakadiAvroSchemaService
                .getNakadiAvroSchemas(BATCH_PUBLISHING)));
    }

    @RequestMapping(value = BATCH_PUBLISHING_PATH + "/{version}", method = RequestMethod.GET)
    public ResponseEntity<?> getNakadiBatchPublishingAvroSchemasFiltered(
            @PathVariable("version") final String version) {
        return ResponseEntity.ok(nakadiAvroSchemaService.getNakadiAvroSchemas(BATCH_PUBLISHING, version));
    }

    @RequestMapping(value = BATCH_CONSUMPTION_PATH, method = RequestMethod.GET)
    public ResponseEntity<?> getNakadiBatchConsumptionAvroSchemas() {
        return ResponseEntity.ok(new ItemsWrapper<>(nakadiAvroSchemaService
                .getNakadiAvroSchemas(BATCH_CONSUMPTION)));
    }

    @RequestMapping(value = BATCH_CONSUMPTION_PATH + "/{version}", method = RequestMethod.GET)
    public ResponseEntity<?> getNakadiBatchConsumptionAvroSchemasFiltered(
            @PathVariable("version") final String version) {
        return ResponseEntity.ok(nakadiAvroSchemaService.getNakadiAvroSchemas(BATCH_CONSUMPTION, version));
    }
}
