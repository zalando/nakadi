package org.zalando.nakadi.filters;

import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import org.springframework.http.HttpHeaders;
import org.springframework.web.filter.OncePerRequestFilter;
import org.zalando.nakadi.service.TracingService;

import javax.servlet.AsyncEvent;
import javax.servlet.AsyncListener;
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

public class TracingFilter extends OncePerRequestFilter {

    private static final String GENERIC_OPERATION_NAME = "generic_request";
    private static final String SPAN_CONTEXT_HEADER = "span_ctx";

    public TracingFilter() {
    }

    private class AsyncRequestSpanFinalizer implements AsyncListener {

        private final Span span;
        private final HttpServletRequest request;
        private final HttpServletResponse response;

        private AsyncRequestSpanFinalizer(final Span span,
                                          final HttpServletRequest request,
                                          final HttpServletResponse response) {
            this.span = span;
            this.request = request;
            this.response = response;
        }

        private void finalizeSpan() {
            try {
                traceRequest(span, request, response);
            } finally {
                span.finish();
            }
        }

        @Override
        public void onComplete(final AsyncEvent event) {
            finalizeSpan();
        }

        @Override
        public void onTimeout(final AsyncEvent event) {
            finalizeSpan();
        }

        @Override
        public void onError(final AsyncEvent event) {
            finalizeSpan();
        }

        @Override
        public void onStartAsync(final AsyncEvent event) {
        }
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
            spanBuilder = TracingService.buildNewChildSpan(GENERIC_OPERATION_NAME, spanContext);
        } else {
            spanBuilder = TracingService.buildNewSpan(GENERIC_OPERATION_NAME);
        }
        spanBuilder
                .withTag("http.url", request.getRequestURI() +
                         Optional.ofNullable(request.getQueryString()).map(q -> "?" + q).orElse(""))

                .withTag("http.header.content_encoding",
                         Optional.ofNullable(request.getHeader(HttpHeaders.CONTENT_ENCODING)).orElse("-"))

                .withTag("http.header.accept_encoding",
                         Optional.ofNullable(request.getHeader(HttpHeaders.ACCEPT_ENCODING)).orElse("-"))

                .withTag("http.header.user_agent",
                         Optional.ofNullable(request.getHeader(HttpHeaders.USER_AGENT)).orElse("-"));

        final Span span = spanBuilder.start();
        try (Closeable ignored = TracingService.activateSpan(span)) {
            filterChain.doFilter(request, response);
        } catch (final Exception ex) {
            TracingService.setErrorFlag(span);
            TracingService.logError(span, ex);
            throw ex;
        } finally {
            response.setHeader(SPAN_CONTEXT_HEADER,
                    TracingService.getTextMapFromSpanContext(span.context()).toString());

            if (request.isAsyncStarted()) {
                request.getAsyncContext().addListener(new AsyncRequestSpanFinalizer(span, request, response));
            } else {
                try {
                    traceRequest(span, request, response);
                } finally {
                    span.finish();
                }
            }
        }
    }

    private static void traceRequest(final Span span,
                                     final HttpServletRequest request,
                                     final HttpServletResponse response) {

        final int statusCode = response.getStatus();
        span.setTag("http.status_code", statusCode);
        if (statusCode >= 500) {
            // controllers may also set the error flag for other status codes, but we won't overwrite it here
            TracingService.setErrorFlag(span);
        }

        // content length might not be known before the request was consumed, so set it after handling
        span.setTag("content_length", request.getContentLengthLong());
    }
}
