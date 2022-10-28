package org.zalando.nakadi.util;

import org.slf4j.MDC;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;

public final class MDCUtils {

    public static final String FLOW_ID = "flowId";
    public static final String SUBSCRIPTION_ID = "subscriptionId";
    public static final String SUBSCRIPTION_STREAM_ID = "streamId";

    public interface CloseableNoEx extends Closeable {
        @Override
        void close();
    }

    public static class Context {
        @Nullable
        private final Map<String, String> values;

        public Context(final Map<String, String> values) {
            this.values = values;
        }
    }

    private MDCUtils() {
    }

    public static Context getContext() {
        return new Context(MDC.getCopyOfContextMap());
    }

    public static CloseableNoEx withContext(final Context context) {
        final Map<String, String> oldContextMap = MDC.getCopyOfContextMap();
        MDC.setContextMap(null == context.values ? new HashMap<>() : context.values);
        return () -> {
            MDC.setContextMap(null == oldContextMap ? new HashMap<>() : oldContextMap);
        };
    }

    public static CloseableNoEx withFlowId(final String flowId) {
        return withSingleMDCValue(FLOW_ID, flowId);
    }

    public static CloseableNoEx withSubscriptionId(final String subscriptionId) {
        return withSingleMDCValue(SUBSCRIPTION_ID, subscriptionId);
    }

    public static CloseableNoEx withSubscriptionIdStreamId(final String subscriptionId, final String streamId) {
        final String oldSubscriptionId = MDC.get(SUBSCRIPTION_ID);
        final String oldStreamID = MDC.get(SUBSCRIPTION_STREAM_ID);

        MDC.put(SUBSCRIPTION_ID, maskNull(subscriptionId));
        MDC.put(SUBSCRIPTION_STREAM_ID, maskNull(streamId));

        return () -> {
            restoreValue(SUBSCRIPTION_STREAM_ID, oldStreamID);
            restoreValue(SUBSCRIPTION_ID, oldSubscriptionId);
        };
    }

    private static CloseableNoEx withSingleMDCValue(final String key, final String value) {
        final String oldValue = MDC.get(key);
        MDC.put(key, maskNull(value));
        return () -> restoreValue(key, oldValue);
    }

    private static void restoreValue(final String key, final String oldValue) {
        if (null == oldValue) {
            MDC.remove(key);
        } else {
            MDC.put(key, oldValue);
        }
    }

    private static String maskNull(final String valueIn) {
        return null == valueIn ? "NULL" : valueIn;
    }

    // The method is incorrectly used, as there is a mix of user-provided flow id and service generated.
    // The method should be transformed into more direct instruction to publisher to use specific flow id, extraction
    // of the value from logging seems very weird
    public static String getFlowId() {
        return MDC.get(FLOW_ID);
    }
}
