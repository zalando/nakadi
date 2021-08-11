package org.zalando.nakadi.service;

import com.google.common.collect.ImmutableMap;
import io.opentracing.References;
import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.propagation.TextMapAdapter;
import io.opentracing.util.GlobalTracer;

import javax.servlet.http.HttpServletRequest;
import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;

import static io.opentracing.propagation.Format.Builtin.HTTP_HEADERS;
import static io.opentracing.propagation.Format.Builtin.TEXT_MAP;

public class TracingService {
    private static final String BUCKET_NAME_5_50_KB = "5K-50K";
    private static final String BUCKET_NAME_5_KB = "<5K";
    private static final String BUCKET_NAME_MORE_THAN_50_KB = ">50K";

    private static final Long BUCKET_5_KB = 5000L;
    private static final Long BUCKET_MORE_THAN_50_KB = 50000L;

    public static String getSLOBucket(final long batchSize) {
        if (batchSize > BUCKET_MORE_THAN_50_KB) {
            return BUCKET_NAME_MORE_THAN_50_KB;
        } else if (batchSize < BUCKET_5_KB) {
            return BUCKET_NAME_5_KB;
        }
        return BUCKET_NAME_5_50_KB;
    }

    public static void logErrorInSpan(final Span span, final String error) {
        if (error != null) {
            span.log(ImmutableMap.of("error.description", error));
        }
    }

    public static void logErrorInSpan(final Span span, final Exception ex) {
        if (ex.getMessage() != null) {
            span.log(ImmutableMap.of("error.description", ex.getMessage()));
        } else {
            span.log(ImmutableMap.of("error.description", ex.toString()));
        }
    }

    public static Span extractSpan(final HttpServletRequest request, final String operation) {
        final Span span = (Span) request.getAttribute("span");
        if (span != null) {
            return span.setOperationName(operation);
        }
        return GlobalTracer.get().buildSpan("default_Span").start();
    }

    public static Tracer.SpanBuilder buildNewSpan(final String operationName) {
        return GlobalTracer.get().buildSpan(operationName);
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

    public static SpanContext extractFromRequestHeaders(final Map<String, String> requestHeaders) {
        return GlobalTracer.get().extract(HTTP_HEADERS, new TextMapAdapter(requestHeaders));
    }

    public static Map<String, String> getTextMapFromSpanContext(final SpanContext spanContext) {
        final Map<String, String> textMap = new HashMap<>();
        GlobalTracer.get().inject(spanContext, TEXT_MAP, new TextMapAdapter(textMap));
        return textMap;
    }
}
