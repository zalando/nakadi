package org.zalando.nakadi.service;

import com.google.common.collect.ImmutableMap;
import io.opentracing.References;
import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.util.GlobalTracer;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import java.io.Closeable;
import java.util.concurrent.TimeUnit;

public class TracingService {

    private static final String BUCKET_NAME_5_50_KB = "5K-50K";
    private static final String BUCKET_NAME_5_KB = "<5K";
    private static final String BUCKET_NAME_MORE_THAN_50_KB = ">50K";

    private static final Long BUCKET_5_KB = 5000L;
    private static final Long BUCKET_MORE_THAN_50_KB = 50000L;

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

    public static void logStreamCloseReason(final String error) {
        final Span currentActiveSpan = getCurrentActiveSpan();
        if (null != currentActiveSpan && error != null) {
            currentActiveSpan.log(ImmutableMap.of("stream.close.reason", error));
        }
    }

    public static Span extractSpan(final HttpServletRequest request, final String operation) {
        final Span span = (Span) request.getAttribute("span");
        if (span != null) {
            return span.setOperationName(operation);
        }
        return GlobalTracer.get().buildSpan("default_Span").start();
    }

    public static Span getCurrentActiveSpan() {
        return GlobalTracer.get().activeSpan();
    }

    public static Span getNewSpanWithReference(final String operationName, final Long timeStamp,
                                               final SpanContext referenceSpanContext) {
        return GlobalTracer.get()
                .buildSpan(operationName)
                .addReference(References.FOLLOWS_FROM, referenceSpanContext)
                .withStartTimestamp(TimeUnit.MILLISECONDS.toMicros(timeStamp))
                .start();
    }

    public static Span getNewSpan(final String operationName, final Long timeStamp) {
        return GlobalTracer.get()
                .buildSpan(operationName)
                .withStartTimestamp(TimeUnit.MILLISECONDS.toMicros(timeStamp))
                .ignoreActiveSpan().start();
    }

    public static Span getNewSpanWithParent(final String operationName, final Long timeStamp,
                                            final Span span) {
        return GlobalTracer.get()
                .buildSpan(operationName)
                .withStartTimestamp(TimeUnit.MILLISECONDS.toMicros(timeStamp))
                .asChildOf(span).start();
    }

    public static Span getNewSpanWithParent(final String operationName, final Long timeStamp,
                                            final SpanContext spanContext) {
        return GlobalTracer.get()
                .buildSpan(operationName)
                .withStartTimestamp(TimeUnit.MILLISECONDS.toMicros(timeStamp))
                .asChildOf(spanContext).start();
    }

    public static Span getNewSpanWithParent(final Span span, final String operationName) {
        return GlobalTracer.get()
                .buildSpan(operationName)
                .asChildOf(span).start();
    }

    public static String getSLOBucket(final long batchSize) {
        if (batchSize > BUCKET_MORE_THAN_50_KB) {
            return BUCKET_NAME_MORE_THAN_50_KB;
        } else if (batchSize < BUCKET_5_KB) {
            return BUCKET_NAME_5_KB;
        }
        return BUCKET_NAME_5_50_KB;
    }

    public static Tracer.SpanBuilder getNewSpanBuilder(
            final String operationName,
            @Nullable
            final Span referenceSpan) {
        final Tracer.SpanBuilder spanBuilder = GlobalTracer.get()
                .buildSpan(operationName)
                .withStartTimestamp(TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis()));
        if (null != referenceSpan) {
            spanBuilder.addReference(References.FOLLOWS_FROM, referenceSpan.context());
        }
        return spanBuilder;
    }

    public static Closeable withActiveSpan(final Tracer.SpanBuilder spanBuilder) {
        final Span newSpan = spanBuilder.start();
        final Closeable activation;
        try {
            activation = activateScope(newSpan);
        } catch (RuntimeException ex) {
            try {
                newSpan.finish();
            } finally {
                throw ex;
            }
        }
        return () -> {
            try {
                activation.close();
            } finally {
                newSpan.finish();
            }
        };

    }


    public static Closeable activateScope(final Span span) {
        return GlobalTracer.get().scopeManager().activate(span, false);
    }

}
