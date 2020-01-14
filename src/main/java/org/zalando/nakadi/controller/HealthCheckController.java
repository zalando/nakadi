package org.zalando.nakadi.controller;

import org.eclipse.jetty.http.HttpStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import static org.springframework.http.MediaType.TEXT_PLAIN_VALUE;
import static org.springframework.http.ResponseEntity.ok;
import static org.springframework.web.bind.annotation.RequestMethod.GET;

@RestController
@RequestMapping(value = "/health", produces = TEXT_PLAIN_VALUE)
public class HealthCheckController {

    private final TerminationService terminationService;

    @Autowired
    public HealthCheckController(final TerminationService terminationService) {
        this.terminationService = terminationService;
    }

    @RequestMapping(method = GET)
    public ResponseEntity<String> healthCheck() {
        if (terminationService.isTerminating()) {
            return ResponseEntity.status(HttpStatus.IM_A_TEAPOT_418).build();
        }
        return ok().body("OK");
    }
}
