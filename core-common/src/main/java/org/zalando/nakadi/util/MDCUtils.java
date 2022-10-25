package org.zalando.nakadi.util;

import org.slf4j.MDC;

import java.io.Closeable;

public final class MDCUtils {

    public static final String FLOW_ID = "flowId";

    public interface CloseableNOException extends Closeable {
        @Override
        void close();
    }

    private MDCUtils() {
    }

    public static CloseableNOException withFlowId(final String flowId) {
        return withMdcField("flowId", flowId);
    }

    public static CloseableNOException withMdcField(final String key, final String valueIn) {
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

    public static String getFlowId() {
        return MDC.get(FLOW_ID);
    }
}
