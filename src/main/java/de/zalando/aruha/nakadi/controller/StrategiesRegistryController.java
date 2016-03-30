package de.zalando.aruha.nakadi.controller;

import de.zalando.aruha.nakadi.domain.PartitionStrategyDescriptor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

import static de.zalando.aruha.nakadi.service.StrategiesRegistry.AVAILABLE_PARTITION_STRATEGIES;
import static org.springframework.http.ResponseEntity.status;

@RestController
@RequestMapping(value = "/registry")
public class StrategiesRegistryController {

    @RequestMapping(value = "/partition-strategies", method = RequestMethod.GET)
    public ResponseEntity<List<PartitionStrategyDescriptor>> listPartitionStrategies() {
        return status(HttpStatus.OK).body(AVAILABLE_PARTITION_STRATEGIES);
    }

}
