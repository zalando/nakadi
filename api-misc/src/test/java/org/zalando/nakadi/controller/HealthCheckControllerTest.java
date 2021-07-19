package org.zalando.nakadi.controller;

import org.junit.Assert;
import org.junit.Test;
import org.springframework.http.HttpStatus;
import org.zalando.nakadi.ShutdownHooks;

import java.util.concurrent.atomic.AtomicBoolean;

public class HealthCheckControllerTest {

    @Test
    public void testHealthControllerReports503OnShutdown() {
        final ShutdownHooks shutdownHooks = new ShutdownHooks();
        final AtomicBoolean shuttingDown = new AtomicBoolean(false);
        final HealthCheckController controller = new HealthCheckController(shutdownHooks, shuttingDown);
        controller.postConstruct();
        Assert.assertEquals(
                HttpStatus.OK,
                controller.healthCheck().getStatusCode());

        shuttingDown.set(true);
        Assert.assertEquals(
                HttpStatus.SERVICE_UNAVAILABLE,
                controller.healthCheck().getStatusCode());
    }
}
