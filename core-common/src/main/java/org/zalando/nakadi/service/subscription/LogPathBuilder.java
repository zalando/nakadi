package org.zalando.nakadi.service.subscription;

public class LogPathBuilder {
    public static String build(final String subscriptionId, final String addition) {
        return "s." + subscriptionId + "." + addition;
    }

    public static String build(final String subscriptionId, final String streamId, final String addition) {
        return build(subscriptionId, streamId + "." + addition);
    }
}
