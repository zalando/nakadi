package org.zalando.nakadi.util;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.log4j.NDC;

/**
 * Helper class to deal with FlowIds.
 *
 * NOTE: It uses log4j's NDC and hence is bound to log4j (behind slf4j). If you change to another logging framework
 * you have to rework this class.
 */
public final class FlowIdUtils {

    private FlowIdUtils() {
    }

    public static void push(final String flowId) {
        NDC.push(flowId);
    }

    public static String generateFlowId() {
        return RandomStringUtils.randomAlphanumeric(24);
    }

    public static void clear() {
        while (!"".equals(NDC.pop())) {}
    }

    public static String peek() {
        return NDC.peek();
    }

    public static String pop() {
        return NDC.pop();
    }


}
