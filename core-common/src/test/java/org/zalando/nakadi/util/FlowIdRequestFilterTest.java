package org.zalando.nakadi.util;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

public class FlowIdRequestFilterTest {
    private static final Logger LOG = LoggerFactory.getLogger(FlowIdRequestFilterTest.class);

    @Test
    public void testLogging() {
        for (int i = 0; i < 10; ++i) {

            MDC.put("flowId", "123");
            try {
                LOG.error("Failed?");
            } finally {
                MDC.remove("flowId");
            }
        }
    }

}