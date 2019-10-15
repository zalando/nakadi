package org.zalando.nakadi.filters;

import com.google.common.collect.ImmutableMap;
import com.google.common.net.HttpHeaders;
import io.opentracing.Scope;
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

    private class RequestInfo {

        private String userAgent;
        private String user;
        private String method;
        private String path;
        private String query;
        private String contentEncoding;
        private Long contentLength;
        private String acceptEncoding;
        private Long requestTime;

        private RequestInfo(final HttpServletRequest request, final long requestTime) {
            this.userAgent = Optional.ofNullable(request.getHeader("User-Agent")).orElse("-");
            this.user = authorizationService.getSubject().map(Subject::getName).orElse("-");
            this.method = request.getMethod();
            this.path = request.getRequestURI();
            this.query = Optional.ofNullable(request.getQueryString()).map(q -> "?" + q).orElse("");
            this.contentEncoding = Optional.ofNullable(request.getHeader(HttpHeaders.CONTENT_ENCODING)).orElse("-");
            this.acceptEncoding = Optional.ofNullable(request.getHeader(HttpHeaders.ACCEPT_ENCODING)).orElse("-");
            this.contentLength = request.getContentLengthLong() == -1 ? 0 : request.getContentLengthLong();
            this.requestTime = requestTime;
        }
    }

    @Autowired
    public TracingFilter(final AuthorizationService authorizationService) {

        this.authorizationService = authorizationService;
    }

    private class AsyncRequestListener implements AsyncListener {
        private final HttpServletResponse response;
        private final String flowId;
        private final RequestInfo requestLogInfo;
        private final Span currentSpan;

        private AsyncRequestListener(final HttpServletRequest request, final HttpServletResponse response,
                                     final long startTime, final String flowId, final Span span) {
            this.response = response;
            this.flowId = flowId;
            this.requestLogInfo = new RequestInfo(request, startTime);
            this.currentSpan = span;
        }

        private void logOnEvent() {
            FlowIdUtils.push(this.flowId);
            traceRequest(this.requestLogInfo, this.response.getStatus(), currentSpan);
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
        final RequestInfo requestInfo = new RequestInfo(request, System.currentTimeMillis());

        final Map<String, String> requestHeaders = Collections.list(request.getHeaderNames())
                .stream()
                .collect(Collectors.toMap(h -> h, request::getHeader));

        final SpanContext spanContext = GlobalTracer.get()
                .extract(HTTP_HEADERS, new TextMapExtractAdapter(requestHeaders));
        final Span baseSpan;
        if (spanContext != null) {
            if (isCommitRequest(requestInfo)) {
                baseSpan = TracingService.getNewSpanWithReference("commit_events",
                        requestInfo.requestTime, spanContext);
            } else {
                baseSpan = TracingService.getNewSpanWithParent("default",
                        requestInfo.requestTime, spanContext);
            }
        } else {
            baseSpan = TracingService.getNewSpan("default", requestInfo.requestTime);
        }

        try {
            final Scope scope = TracingService.activateSpan(baseSpan, false);
            TracingService.setCustomTags(scope.span(),
                    ImmutableMap.<String, Object>builder()
                            .put("client_id", requestInfo.user)
                            .put("http.url", requestInfo.path + requestInfo.query)
                            .put("http.header.content_encoding", requestInfo.contentEncoding)
                            .put("http.header.accept_encoding", requestInfo.acceptEncoding)
                            .put("http.header.user_agent", requestInfo.userAgent)
                            .build());
            request.setAttribute("span", baseSpan);
            //execute request
            filterChain.doFilter(request, response);
            if (request.isAsyncStarted()) {
                final String flowId = FlowIdUtils.peek();
                request.getAsyncContext().addListener(new AsyncRequestListener(request, response,
                        requestInfo.requestTime, flowId, baseSpan));
            }
        } finally {
            if (!request.isAsyncStarted()) {
                traceRequest(requestInfo, response.getStatus(), baseSpan);
            }
            final Map<String, String> spanContextToInject = new HashMap<>();
            GlobalTracer.get().inject(baseSpan.context(),
                    HTTP_HEADERS, new TextMapInjectAdapter(spanContextToInject));
            response.setHeader(SPAN_CONTEXT, spanContextToInject.toString());
            baseSpan.finish();
        }
    }


    private boolean isCommitRequest(final RequestInfo requestInfo) {
        return (requestInfo.path != null && "POST".equals(requestInfo.method) &&
                requestInfo.path.startsWith("/subscriptions/") &&
                (requestInfo.path.endsWith("/cursors") || requestInfo.path.endsWith("/cursors/")));
    }

    private void traceRequest(final RequestInfo requestLogInfo, final int statusCode,
                              final Span span) {
        final Scope scope = TracingService.activateSpan(span, false);
        final Map<String, Object> tags = new HashMap<String, Object>() {{
            put("http.status_code", statusCode);
            put("content_length", requestLogInfo.contentLength);
        }};
        tags.put("error", statusCode == 207 || statusCode >= 500);
        TracingService.setCustomTags(scope.span(), tags);
    }
}
