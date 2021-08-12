package org.zalando.nakadi.filters;

import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.plugin.api.authz.Subject;
import org.zalando.nakadi.service.TracingService;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@Component
public class TracingFilter extends OncePerRequestFilter {

    private static final String SPAN_CONTEXT = "span_ctx";
    private final AuthorizationService authorizationService;


    @Autowired
    public TracingFilter(final AuthorizationService authorizationService) {
        this.authorizationService = authorizationService;
    }

    @Override
    protected void doFilterInternal(final HttpServletRequest request,
                                    final HttpServletResponse response,
                                    final FilterChain filterChain)
            throws IOException, ServletException {

        final Tracer.SpanBuilder spanBuilder;

        final Map<String, String> requestHeaders = Collections.list(request.getHeaderNames())
                .stream()
                .collect(Collectors.toMap(h -> h, request::getHeader));

        final SpanContext spanContext = TracingService.extractFromRequestHeaders(requestHeaders);
        if (spanContext != null) {
            spanBuilder = TracingService.buildNewFollowerSpan("all_requests", spanContext);
        } else {
            spanBuilder = TracingService.buildNewSpan("all_requests");
        }
        spanBuilder
                .withTag("content_length", request.getContentLength())
                .withTag("http.url", request.getRequestURI() +
                         Optional.ofNullable(request.getQueryString()).map(q -> "?" + q).orElse(""))
                .withTag("http.header.content_encoding",
                         Optional.ofNullable(request.getQueryString()).map(q -> "?" + q).orElse(""))
                .withTag("http.header.accept_encoding",
                         Optional.ofNullable(request.getQueryString()).map(q -> "?" + q).orElse(""))
                .withTag("http.header.user_agent",
                         Optional.ofNullable(request.getHeader("User-Agent")).orElse("-"));

        final Span span = spanBuilder.start();
        try (Closeable ignored = TracingService.activateSpan(span)) {
            span.setTag("client_id", authorizationService.getSubject().map(Subject::getName).orElse("-"));

            //execute request
            filterChain.doFilter(request, response);

            response.setHeader(SPAN_CONTEXT, TracingService.getTextMapFromSpanContext(span.context()).toString());

            final int statusCode = response.getStatus();
            span.setTag("http.status_code", statusCode);
            if (statusCode >= 500) {
                // controllers may also set the error flag for other status codes, but we won't overwrite it here
                TracingService.setErrorFlag();
            }
        } finally {
            span.finish();
        }
    }
}
