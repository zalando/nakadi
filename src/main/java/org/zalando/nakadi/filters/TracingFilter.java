package org.zalando.nakadi.filters;

import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.propagation.TextMapExtractAdapter;
import io.opentracing.propagation.TextMapInjectAdapter;
import io.opentracing.util.GlobalTracer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.plugin.api.authz.Subject;
import org.zalando.nakadi.service.TracingService;
import org.zalando.nakadi.util.FlowIdUtils;

import javax.servlet.AsyncEvent;
import javax.servlet.AsyncListener;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.opentracing.propagation.Format.Builtin.HTTP_HEADERS;

@Component
public class TracingFilter extends OncePerRequestFilter {

    private static final String SPAN_CONTEXT = "span_ctx";
    private final AuthorizationService authorizationService;


    @Autowired
    public TracingFilter(final AuthorizationService authorizationService) {
        this.authorizationService = authorizationService;
    }

    private class AsyncRequestListener implements AsyncListener {
        private final HttpServletResponse response;
        private final String flowId;
        final long contentLength;
        private final Span currentSpan;

        private AsyncRequestListener(final HttpServletRequest request, final HttpServletResponse response,
                                     final String flowId, final Span span) {
            this.response = response;
            this.flowId = flowId;
            this.currentSpan = span;
            this.contentLength = request.getContentLengthLong() == -1 ? 0 : request.getContentLengthLong();
        }

        private void logOnEvent() {
            FlowIdUtils.push(this.flowId);
            traceRequest(contentLength, this.response.getStatus(), currentSpan);
            FlowIdUtils.clear();
        }

        @Override
        public void onComplete(final AsyncEvent event) {
            logOnEvent();
        }

        @Override
        public void onTimeout(final AsyncEvent event) {
            logOnEvent();
        }

        @Override
        public void onError(final AsyncEvent event) {
            logOnEvent();
        }

        @Override
        public void onStartAsync(final AsyncEvent event) {

        }
    }

    @Override
    protected void doFilterInternal(final HttpServletRequest request,
                                    final HttpServletResponse response, final FilterChain filterChain)
            throws IOException, ServletException {
        final Long startTime = System.currentTimeMillis();
        final Map<String, String> requestHeaders = Collections.list(request.getHeaderNames())
                .stream()
                .collect(Collectors.toMap(h -> h, request::getHeader));

        final SpanContext spanContext = GlobalTracer.get()
                .extract(HTTP_HEADERS, new TextMapExtractAdapter(requestHeaders));
        final Span baseSpan;
        if (spanContext != null) {
            if (isCommitRequest(request.getRequestURI(), request.getMethod())) {
                baseSpan = TracingService.getNewSpanWithReference("commit_events",
                        startTime, spanContext);
            } else {
                baseSpan = TracingService.getNewSpanWithParent("all_requests",
                        startTime, spanContext);
            }
        } else {
            baseSpan = TracingService.getNewSpan("all_requests", startTime);
        }

        try {
            TracingService.activateSpan(baseSpan, false)
                    .setTag("client_id", authorizationService.getSubject().map(Subject::getName).orElse("-"))
                    .setTag("http.url", request.getRequestURI() +
                            Optional.ofNullable(request.getQueryString()).map(q -> "?" + q).orElse(""))
                    .setTag("http.header.content_encoding",
                            Optional.ofNullable(request.getQueryString()).map(q -> "?" + q).orElse(""))
                    .setTag("http.header.accept_encoding",
                            Optional.ofNullable(request.getQueryString()).map(q -> "?" + q).orElse(""))
                    .setTag("http.header.user_agent",
                            Optional.ofNullable(request.getHeader("User-Agent")).orElse("-"));
            request.setAttribute("span", baseSpan);
            //execute request
            filterChain.doFilter(request, response);
            if (request.isAsyncStarted()) {
                final String flowId = FlowIdUtils.peek();
                request.getAsyncContext().addListener(new AsyncRequestListener(request, response, flowId, baseSpan));
            }
        } finally {
            if (!request.isAsyncStarted()) {
                traceRequest(request.getContentLength(), response.getStatus(), baseSpan);
            }
            final Map<String, String> spanContextToInject = new HashMap<>();
            GlobalTracer.get().inject(baseSpan.context(),
                    HTTP_HEADERS, new TextMapInjectAdapter(spanContextToInject));
            response.setHeader(SPAN_CONTEXT, spanContextToInject.toString());
            baseSpan.finish();
        }
    }


    private boolean isCommitRequest(final String path, final String method) {
        return (path != null && "POST".equals(method) &&
                path.startsWith("/subscriptions/") &&
                (path.endsWith("/cursors") || path.endsWith("/cursors/")));
    }

    private void traceRequest(final long contentLength, final int statusCode,
                              final Span span) {
        TracingService.activateSpan(span, false)
                .setTag("http.status_code", statusCode)
                .setTag("content_length", contentLength)
                .setTag("error", statusCode == 207 || statusCode >= 500);
    }
}
