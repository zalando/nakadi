package de.zalando.aruha.nakadi.controller;

import de.zalando.aruha.nakadi.domain.PartitionResolutionStrategy;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

import static de.zalando.aruha.nakadi.service.Registry.AVAILABLE_PARTITIONING_STRATEGIES;
import static org.springframework.http.ResponseEntity.status;

@RestController
@RequestMapping(value = "/registry")
public class RegistryController {

    @RequestMapping(value = "/partitioning-strategies", method = RequestMethod.GET)
    public ResponseEntity<List<PartitionResolutionStrategy>> listPartitioningStrategies() {
        return status(HttpStatus.OK).body(AVAILABLE_PARTITIONING_STRATEGIES);
    }

}
