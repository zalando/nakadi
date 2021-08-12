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

import javax.servlet.AsyncContext;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;
import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@Component
public class TracingFilter extends OncePerRequestFilter {

    private static final String GENERIC_OPERATION_NAME = "generic_request";
    private static final String SPAN_CONTEXT = "span_ctx";
    private final AuthorizationService authorizationService;

    @Autowired
    public TracingFilter(final AuthorizationService authorizationService) {
        this.authorizationService = authorizationService;
    }

    private class AsyncContextTracingWrapper extends AsyncContextWrapper {

        private final SpanContext referenceTracingContext;

        private AsyncContextTracingWrapper(final AsyncContext context, final SpanContext referenceTracingContext) {
            super(context);
            this.referenceTracingContext = referenceTracingContext;
        }

        @Override
        public void start(final Runnable runnable) {
            this.context.start(() -> {
                    final Span span = TracingService.buildNewFollowerSpan("async_request", referenceTracingContext)
                            .start();

                    try (Closeable ignored = TracingService.activateSpan(span)) {
                        runnable.run();
                    } catch (final IOException ioe) {
                        throw new RuntimeException(ioe);
                    } finally {
                        traceRequest(span, (HttpServletRequest) getRequest(), (HttpServletResponse) getResponse());
                        span.finish();
                    }
                });
        }
    }

    private class AsyncRequestTracingWrapper extends HttpServletRequestWrapper {

        private final HttpServletRequest request;
        private final SpanContext referenceTracingContext;

        private AsyncRequestTracingWrapper(final HttpServletRequest request,
                                           final SpanContext referenceTracingContext) {
            super(request);
            this.request = request;
            this.referenceTracingContext = referenceTracingContext;
        }

        @Override
        public AsyncContext startAsync() throws IllegalStateException {
            final AsyncContext asyncContext = request.startAsync();
            return new AsyncContextTracingWrapper(asyncContext, referenceTracingContext);
        }

        @Override
        public AsyncContext startAsync(final ServletRequest servletRequest, final ServletResponse servletResponse)
            throws IllegalStateException {
            final AsyncContext asyncContext = request.startAsync(servletRequest, servletResponse);
            return new AsyncContextTracingWrapper(asyncContext, referenceTracingContext);
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
            spanBuilder = TracingService.buildNewFollowerSpan(GENERIC_OPERATION_NAME, spanContext);
        } else {
            spanBuilder = TracingService.buildNewSpan(GENERIC_OPERATION_NAME);
        }
        spanBuilder
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
            final AsyncRequestTracingWrapper requestWrapper = new AsyncRequestTracingWrapper(request, span.context());
            filterChain.doFilter(requestWrapper, response);

            if (!request.isAsyncStarted()) {
                traceRequest(span, request, response);
            }

            response.setHeader(SPAN_CONTEXT, TracingService.getTextMapFromSpanContext(span.context()).toString());
        } finally {
            span.finish();
        }
    }

    private static void traceRequest(final Span span, final HttpServletRequest request,
                                     final HttpServletResponse response) {

        final int statusCode = response.getStatus();
        span.setTag("http.status_code", statusCode);
        if (statusCode >= 500) {
            // controllers may also set the error flag for other status codes, but we won't overwrite it here
            TracingService.setErrorFlag();
        }

        // content length might not be known before the request was consumed, so set it after handling
        span.setTag("content_length", request.getContentLengthLong());
    }
}
