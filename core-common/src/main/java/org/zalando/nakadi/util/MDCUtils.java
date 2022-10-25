package org.zalando.nakadi.util;

import org.slf4j.MDC;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;

public final class MDCUtils {

    public static final String FLOW_ID = "flowId";

    public interface CloseableNoEx extends Closeable {
        @Override
        void close();
    }

    public static class MDCContext {
        @Nullable
        private final Map<String, String> values;

        public MDCContext(final Map<String, String> values) {
            this.values = values;
        }
    }

    private MDCUtils() {
    }

    public static MDCContext getContext() {
        return new MDCContext(MDC.getCopyOfContextMap());
    }

    public static CloseableNoEx withContext(final MDCContext context) {
        // TODO!!!!
        final Map<String, String> oldContextMap = MDC.getCopyOfContextMap();
        final Map<String, String> newContextMap = null == oldContextMap ? new HashMap<>() : new HashMap<>(oldContextMap);
        newContextMap.putAll(context.values);
        MDC.setContextMap(newContextMap);
        return () -> MDC.setContextMap(oldContextMap);
    }

    public static CloseableNoEx withFlowId(final String flowId) {
        return withMdcField(FLOW_ID, flowId);
    }

    public static CloseableNoEx withSubscriptionId(final String subscriptionId) {
        return withMdcField("subscriptionId", subscriptionId);
    }

    public static CloseableNoEx withSubscriptionIdStreamId(final String subscriptionId, final String streamId) {
        final CloseableNoEx closeableOuter = withMdcField("subscriptionId", subscriptionId);
        final CloseableNoEx closeableInner;
        try {
            closeableInner = withMdcField("streamId", streamId);
        } catch (RuntimeException ex) {
            closeableOuter.close();
            throw ex;
        }
        return () -> {
            try {
                closeableInner.close();
            } finally {
                closeableOuter.close();
            }
        };
    }

    public static CloseableNoEx withMdcField(final String key, final String valueIn) {
        final String value = maskNull(valueIn);
        final String oldValue = MDC.get(key);
        MDC.put(key, value);
        return () -> {
            if (null == oldValue) {
                MDC.remove(key);
            } else {
                MDC.put(key, oldValue);
            }
        };
    }

    private static String maskNull(final String valueIn) {
        return null == valueIn ? "NULL" : valueIn;
    }

    // The method is incorrectly used, as there is a mix of user-provided flow id and service generated.
    // The method should be transformed into more direct instruction to publisher to use specific flow id, extraction
    // of the value from logging seems very wierd
    public static String getFlowId() {
        return MDC.get(FLOW_ID);
    }
}
