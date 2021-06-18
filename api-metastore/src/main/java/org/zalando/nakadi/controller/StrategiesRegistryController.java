package org.zalando.nakadi.controller;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.zalando.nakadi.domain.EnrichmentStrategyDescriptor;

import java.util.Arrays;
import java.util.List;

import static org.springframework.http.ResponseEntity.status;
import static org.zalando.nakadi.partitioning.PartitionResolver.ALL_PARTITION_STRATEGIES;

@RestController
@RequestMapping(value = "/registry")
public class StrategiesRegistryController {

    @RequestMapping(value = "/partition-strategies", method = RequestMethod.GET)
    public ResponseEntity<List<String>> listPartitionStrategies() {
        return status(HttpStatus.OK).body(ALL_PARTITION_STRATEGIES);
    }

    @RequestMapping(value = "/enrichment-strategies", method = RequestMethod.GET)
    public ResponseEntity<List<EnrichmentStrategyDescriptor>> listEnrichmentStrategies() {
        final List<EnrichmentStrategyDescriptor> names = Arrays.asList(EnrichmentStrategyDescriptor.values());
        return status(HttpStatus.OK).body(names);
    }

}
