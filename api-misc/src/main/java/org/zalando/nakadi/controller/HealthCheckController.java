package org.zalando.nakadi.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.zalando.nakadi.ShutdownHooks;

import javax.annotation.PostConstruct;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.springframework.http.MediaType.TEXT_PLAIN_VALUE;
import static org.springframework.http.ResponseEntity.ok;
import static org.springframework.http.ResponseEntity.status;
import static org.springframework.web.bind.annotation.RequestMethod.GET;

@RestController
@RequestMapping(value = "/health", produces = TEXT_PLAIN_VALUE)
public class HealthCheckController {

    private static final Logger LOG = LoggerFactory.getLogger(HealthCheckController.class);

    private final ShutdownHooks shutdownHooks;
    private final AtomicBoolean shuttingDown;

    @Autowired
    public HealthCheckController(final ShutdownHooks shutdownHooks) {
        this.shutdownHooks = shutdownHooks;
        this.shuttingDown = new AtomicBoolean(false);
    }

    HealthCheckController(final ShutdownHooks shutdownHooks, final AtomicBoolean shuttingDown) {
        this.shutdownHooks = shutdownHooks;
        this.shuttingDown = shuttingDown;
    }

    @PostConstruct
    public void postConstruct() {
        shutdownHooks.addHook(() -> shuttingDown.set(true));
    }

    @RequestMapping(method = GET)
    public ResponseEntity<String> healthCheck() {
        if (shuttingDown.get()) {
            return status(HttpStatus.SERVICE_UNAVAILABLE).build();
        } else {
            return ok().body("OK");
        }
    }
}
