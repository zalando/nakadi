package org.zalando.nakadi.controller;

import org.junit.Assert;
import org.junit.Test;
import org.springframework.http.HttpStatus;

import java.util.concurrent.atomic.AtomicBoolean;

public class HealthCheckControllerTest {

    @Test
    public void testHealthControllerReports503OnShutdown() {
        final AtomicBoolean shuttingDown = new AtomicBoolean(false);
        final HealthCheckController controller = new HealthCheckController(null, shuttingDown);
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
