package org.zalando.nakadi.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;

public final class MDCUtils {

    public static final String FLOW_ID = "flowId";
    public static final String SUBSCRIPTION_ID = "subscriptionId";
    public static final String SUBSCRIPTION_STREAM_ID = "streamId";

    private static final Logger LOG = LoggerFactory.getLogger(MDCUtils.class);

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

    // Enriches current context with context taken from somewhere else
    public static CloseableNoEx enrichContext(final Context context) {
        if (null == context.values || context.values.isEmpty()) {
            return () -> {
                // do nothing, as we are only enriching the context, not replacing it
            };
        }

        final String[] keyValuesToInject = new String[context.values.size() * 2];
        int idx = 0;
        for (final Map.Entry<String, String> entry : context.values.entrySet()) {
            keyValuesToInject[idx++] = entry.getKey();
            keyValuesToInject[idx++] = entry.getValue();
        }

        return withSeveralMdcValues(keyValuesToInject);
    }

    public static CloseableNoEx withFlowId(final String flowId) {
        return withSeveralMdcValues(FLOW_ID, flowId);
    }

    public static CloseableNoEx withSubscriptionId(final String subscriptionId) {
        return withSeveralMdcValues(SUBSCRIPTION_ID, subscriptionId);
    }

    public static CloseableNoEx withSubscriptionIdStreamId(final String subscriptionId, final String streamId) {
        return withSeveralMdcValues(SUBSCRIPTION_ID, subscriptionId, SUBSCRIPTION_STREAM_ID, streamId);
    }

    public static CloseableNoEx withSeveralMdcValues(final String... keyValues) {
        if (keyValues.length % 2 != 0) {
            throw new RuntimeException("The list is a sequence of key and value, some key has no value");
        }
        final Map<String, String> previousValues = new HashMap<>();
        final CloseableNoEx closer = () -> {
            for (final Map.Entry<String, String> entry : previousValues.entrySet()) {
                try {
                    if (null == entry.getValue()) {
                        MDC.remove(entry.getKey());
                    } else {
                        MDC.put(entry.getKey(), entry.getValue());
                    }
                } catch (RuntimeException ex) {
                    LOG.error("Failed to remove MDC value {}", entry, ex);
                }
            }
        };
        try {
            for (int idx = 0; idx < keyValues.length; idx += 2) {
                final String key = keyValues[idx];
                final String value = maskNull(keyValues[idx + 1]);
                previousValues.put(key, MDC.get(key));
                MDC.put(key, value);
            }
        } catch (RuntimeException ex) {
            LOG.error("Failed to register MDC", ex);
            closer.close();
            throw ex;
        }
        return closer;
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
