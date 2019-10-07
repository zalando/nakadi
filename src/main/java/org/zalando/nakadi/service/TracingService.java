package org.zalando.nakadi.service;

import com.google.common.collect.ImmutableMap;
import com.lightstep.tracer.jre.JRETracer;
import com.lightstep.tracer.shared.Options;
import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.noop.NoopTracerFactory;
import io.opentracing.tag.Tags;
import io.opentracing.util.GlobalTracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;


@Configuration
public class TracingService {
    private static final Logger LOG = LoggerFactory.getLogger(TracingService.class);
    private String componentName = "mycomponent";
    private String accessToken = "test";
    private String collectorHost = "localhost";
    private int collectorPort = 8444;
    public final Tracer tracer;

    @Autowired
    public TracingService() {
        this.tracer = tracer();
    }

    @PostConstruct
    public void registerGlobalTracer() {
        if (!GlobalTracer.isRegistered()) {
            GlobalTracer.register(tracer);
        }
    }

    public Tracer tracer() {
        try {
            final Options options = new Options.OptionsBuilder()
                    .withAccessToken(accessToken)
                    .withCollectorHost(collectorHost)
                    .withCollectorPort(collectorPort)
                    .withComponentName(componentName)
                    .build();

            final Tracer tracer = new JRETracer(options);
            LOG.info("Initialized Lightstep Tracer");
            return tracer;
        } catch (Exception ex) {
            LOG.error("Invalid Lightstep configuration. Returning a NoopTracer. {}", ex.getMessage());
            return NoopTracerFactory.create();
        }
    }

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