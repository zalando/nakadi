package org.zalando.nakadi.service;

import com.google.common.collect.ImmutableMap;
import io.opentracing.References;
import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.tag.Tags;
import io.opentracing.propagation.TextMapAdapter;
import io.opentracing.util.GlobalTracer;

import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;

import static io.opentracing.propagation.Format.Builtin.HTTP_HEADERS;
import static io.opentracing.propagation.Format.Builtin.TEXT_MAP;

public class TracingService {
    private static final String BUCKET_NAME_5_KB = "<5K";
    private static final String BUCKET_NAME_5_50_KB = "5K-50K";
    private static final String BUCKET_NAME_MORE_THAN_50_KB = ">50K";

    private static final long BUCKET_5_KB = 5000L;
    private static final long BUCKET_50_KB = 50000L;

    public static String getSLOBucketName(final long batchSize) {
        if (batchSize > BUCKET_50_KB) {
            return BUCKET_NAME_MORE_THAN_50_KB;
        } else if (batchSize < BUCKET_5_KB) {
            return BUCKET_NAME_5_KB;
        }
        return BUCKET_NAME_5_50_KB;
    }

    public static Tracer.SpanBuilder buildNewSpan(final String operationName) {
        return GlobalTracer.get().buildSpan(operationName);
    }

    public static Tracer.SpanBuilder buildNewChildSpan(final String operationName,
                                                       final SpanContext referenceContext) {
        return buildNewSpan(operationName).addReference(References.CHILD_OF, referenceContext);
    }

    public static Tracer.SpanBuilder buildNewFollowerSpan(final String operationName,
                                                          final SpanContext referenceContext) {
        return buildNewSpan(operationName).addReference(References.FOLLOWS_FROM, referenceContext);
    }

    public static Closeable withActiveSpan(final Tracer.SpanBuilder spanBuilder) {
        final Span span = spanBuilder.start();
        final Closeable scope;
        try {
            scope = GlobalTracer.get().activateSpan(span);
        } catch (final RuntimeException ex) {
            try {
                span.finish();
            } finally {
                throw ex;
            }
        }
        return () -> {
            try {
                scope.close();
            } finally {
                span.finish();
            }
        };
    }

    public static Closeable activateSpan(final Span span) {
        return GlobalTracer.get().activateSpan(span);
    }

    public static Span getActiveSpan() {
        return GlobalTracer.get().activeSpan();
    }

    public static Span setOperationName(final String operationName) {
        return getActiveSpan().setOperationName(operationName);
    }

    public static Span setTag(final String key, final String value) {
        return getActiveSpan().setTag(key, value);
    }

    public static Span setErrorFlag() {
        return setErrorFlag(getActiveSpan());
    }

    public static Span setErrorFlag(final Span span) {
        return span.setTag(Tags.ERROR, true);
    }

    public static void logError(final String error) {
        if (error != null) {
            getActiveSpan().log(ImmutableMap.of("error.description", error));
        }
    }

    public static void logError(final Exception ex) {
        logError(getActiveSpan(), ex);
    }

    public static void logError(final Span span, final Exception ex) {
        if (ex.getMessage() != null) {
            span.log(ImmutableMap.of("error.description", ex.getMessage()));
        } else {
            span.log(ImmutableMap.of("error.description", ex.toString()));
        }
    }

    public static void log(final Map<String, ?> fields) {
        getActiveSpan().log(fields);
    }

    public static SpanContext extractFromRequestHeaders(final Map<String, String> requestHeaders) {
        return GlobalTracer.get().extract(HTTP_HEADERS, new TextMapAdapter(requestHeaders));
    }

    public static Map<String, String> getTextMapFromSpanContext(final SpanContext spanContext) {
        final Map<String, String> textMap = new HashMap<>();
        GlobalTracer.get().inject(spanContext, TEXT_MAP, new TextMapAdapter(textMap));
        return textMap;
    }
}
