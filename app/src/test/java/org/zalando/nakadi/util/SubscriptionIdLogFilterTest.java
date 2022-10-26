package org.zalando.nakadi.util;

import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SubscriptionIdLogFilterTest {
    private static final Logger LOG = LoggerFactory.getLogger("org.zalando.nakadi.service.subscription.test");

    @Ignore
    @Test
    public void testLogging() {
        // Should be logged
        LOG.debug("Without subscription id");
        try (var ignore = MDCUtils.withSubscriptionId("11111111-1111-1111-1111-111111111111")) {
            // Should not be logged
            LOG.debug("With wrong subscription id");
            // Should be logged
            LOG.info("With wrong subscription id, but high level");
        }

        try (var ignore = MDCUtils.withSubscriptionId("00000000-0000-0000-0000-000000000000")) {
            // Should be logged
            LOG.debug("With correct subscription id");
        }
    }
}