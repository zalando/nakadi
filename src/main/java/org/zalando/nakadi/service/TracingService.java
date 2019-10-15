package org.zalando.nakadi.service;

import com.google.common.collect.ImmutableMap;
import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.util.GlobalTracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Configuration;

import java.util.Map;
import java.util.concurrent.TimeUnit;


@Configuration
public class TracingService {
    private static final Logger LOG = LoggerFactory.getLogger(TracingService.class);

    public static void logErrorInSpan(final Scope scope, final String error) {
        if (error != null) {
            scope.span().log(ImmutableMap.of("error:", error));
        }
    }

    public static void setCustomTags(final Span span, final Map<String, Object> tags) {

        for (final Map.Entry<String, Object> entry : tags.entrySet()) {
            if (entry.getValue() instanceof Boolean) {
                span.setTag(entry.getKey(), (Boolean) entry.getValue());
            } else if (entry.getValue() instanceof Number) {
                span.setTag(entry.getKey(), (Number) entry.getValue());
            } else if (entry.getValue() instanceof String) {
                span.setTag(entry.getKey(), (String) entry.getValue());
            } else {
                LOG.warn("Tag is not of the expected type");
                continue;
            }
        }
    }

    public static void setCustomTags(final Scope scope, final Map<String, Object> tags) {

        for (final Map.Entry<String, Object> entry : tags.entrySet()) {
            if (entry.getValue() instanceof Boolean) {
                scope.span().setTag(entry.getKey(), (Boolean) entry.getValue());
            } else if (entry.getValue() instanceof Number) {
                scope.span().setTag(entry.getKey(), (Number) entry.getValue());
            } else if (entry.getValue() instanceof String) {
                scope.span().setTag(entry.getKey(), (String) entry.getValue());
            } else {
                LOG.warn("Tag is not of the expected type");
                continue;
            }
        }
    }

    public static Scope activateSpan(final Span span, final boolean autoCloseSpan) {
        return GlobalTracer.get().scopeManager().activate(span, autoCloseSpan);
    }

    public static Span getNewSpan(final String operationName, final Long timeStamp, final Span parentSpan) {
        return GlobalTracer.get()
                .buildSpan(operationName)
                .asChildOf(parentSpan)
                .withStartTimestamp(TimeUnit.MILLISECONDS.toMicros(timeStamp))
                .start();
    }

    public static Span getNewSpan(final String operationName, final Long timeStamp) {
        return GlobalTracer.get()
                .buildSpan(operationName)
                .withStartTimestamp(TimeUnit.MILLISECONDS.toMicros(timeStamp))
                .ignoreActiveSpan().start();
    }

    public static Span getNewSpan(final String operationName, final Long timeStamp, final SpanContext spanContext) {
        return GlobalTracer.get()
                .buildSpan(operationName)
                .withStartTimestamp(TimeUnit.MILLISECONDS.toMicros(timeStamp))
                .asChildOf(spanContext).start();
    }

}
