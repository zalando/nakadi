package org.zalando.nakadi.controller;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.ws.rs.core.MediaType;

import static org.springframework.http.ResponseEntity.ok;
import static org.springframework.web.bind.annotation.RequestMethod.GET;

@RestController
@RequestMapping(value = "/health", produces = MediaType.TEXT_PLAIN)
public class HealthCheckController {

    @RequestMapping(method = GET)
    public ResponseEntity<String> healthCheck() {
        return ok().body("OK");
    }
}
