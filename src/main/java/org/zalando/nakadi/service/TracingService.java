package org.zalando.nakadi.service;

import com.google.common.collect.ImmutableMap;
import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.tag.Tags;
import io.opentracing.util.GlobalTracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Configuration;


@Configuration
public class TracingService {
    private static final Logger LOG = LoggerFactory.getLogger(TracingService.class);

    public static Scope getScope(final String operationName, final boolean autoCloseSpan, final Span parent,
                                 final String... eventType) {
        final Scope scope = GlobalTracer.get().buildSpan(operationName)
                .asChildOf(parent).startActive(autoCloseSpan);
        if (eventType.length > 0) {
            scope.span().setTag("event_type", eventType[0]);
        }
        return scope;
    }

    public static void setErrorTags(final Scope scope, final String error) {
        Tags.ERROR.set(scope.span(), true);
        if (error != null) {
            scope.span().log(ImmutableMap.of("error:", error));
        }
    }

    public static void setCustomTags(final Scope scope, final String... tags) {
        if (tags.length % 2 != 0) {
            LOG.error("Number of tags don't match the number of values.");
            return;
        }
        for (int i = 0; i < tags.length - 1; i += 2) {
            scope.span().setTag(tags[i], tags[i + 1]);
        }
    }

    public static Scope activateSpan(final Span span) {
        return GlobalTracer.get().scopeManager().activate(span, false);
    }

    public static Scope getScopeIgnoreParent(final String operationName, final boolean autoCloseSpan) {
        return GlobalTracer.get()
                .buildSpan(operationName)
                .ignoreActiveSpan().startActive(autoCloseSpan);
    }

}