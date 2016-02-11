package de.zalando.aruha.nakadi;

import org.apache.log4j.NDC;

import java.security.SecureRandom;
import java.util.Base64;

/**
 * Helper class to deal with FlowIds.
 *
 * NOTE: It uses log4j's NDC and hence is bound to log4j (behind slf4j). If you change to another logging framework
 * you have to rework this class.
 */
public final class FlowIdUtils {

    public static final SecureRandom RANDOM = new SecureRandom();

    private FlowIdUtils() {
    }

    public static void push(final String flowId) {
        NDC.push(flowId);
    }

    public static String generateFlowId() {
        final byte[] bytes = new byte[18];
        RANDOM.nextBytes(bytes);
        return Base64.getEncoder().encodeToString(bytes);
    }

    public static void clear() {
        while (!"".equals(NDC.pop()));
    }

    public static String peek() {
        return NDC.peek();
    }

    public static String pop() {
        return NDC.pop();
    }


}
