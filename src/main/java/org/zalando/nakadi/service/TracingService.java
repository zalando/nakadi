package org.zalando.nakadi.service;

import com.google.common.collect.ImmutableMap;
import io.opentracing.References;
import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.util.GlobalTracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.servlet.http.HttpServletRequest;
import java.util.concurrent.TimeUnit;

@Component
public class TracingService {
    private static final Logger LOG = LoggerFactory.getLogger(TracingService.class);

    public static void logErrorInSpan(final Span span, final String error) {
        if (error != null) {
            span.log(ImmutableMap.of("error.description", error));
        }
    }

    public static void logStreamCloseReason(final Span span, final String error) {
        if (error != null) {
            span.log(ImmutableMap.of("stream.close.reason", error));
        }
    }

    public static void logWarning(final Span span, final String warning) {
        if (warning != null) {
            span.log(ImmutableMap.of("warning:", warning));
        }
    }

    public static Span activateSpan(final Span span, final boolean autoCloseSpan) {
        return GlobalTracer.get().scopeManager().activate(span, autoCloseSpan).span();
    }

    public static Span activateSpan(final HttpServletRequest request, final boolean autoCloseSpan) {
        final Span span = (Span) request.getAttribute("span");
        if (span != null) {
            return GlobalTracer.get().scopeManager().activate(span, autoCloseSpan).span();
        }
        LOG.debug("Starting Default span");
        return GlobalTracer.get().buildSpan("default_Span").startActive(autoCloseSpan).span();
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

}
