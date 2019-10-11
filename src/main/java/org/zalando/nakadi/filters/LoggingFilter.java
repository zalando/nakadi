package org.zalando.nakadi.filters;

import com.google.common.collect.ImmutableMap;
import com.google.common.net.HttpHeaders;
import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.propagation.TextMapExtractAdapter;
import io.opentracing.propagation.TextMapInjectAdapter;
import io.opentracing.util.GlobalTracer;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.plugin.api.authz.Subject;
import org.zalando.nakadi.service.NakadiKpiPublisher;
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
public class LoggingFilter extends OncePerRequestFilter {

    // We are using empty log name, cause it is used only for access log and we do not care about class name
    private static final Logger ACCESS_LOGGER = LoggerFactory.getLogger("ACCESS_LOG");
    private static final String SPAN_CONTEXT = "span_ctx";
    private final NakadiKpiPublisher nakadiKpiPublisher;
    private final String accessLogEventType;
    private final AuthorizationService authorizationService;

    private enum RequestType {
        PUBLISHING("publish_events"),
        COMMIT("commit_events"),
        CONSUMPTION("consume_events"),
        OTHER("other");

        private String operationName;

        public String getOperationName() {
            return this.operationName;
        }

        RequestType(final String operationName) {
            this.operationName = operationName;
        }
    }

    @Autowired
    public LoggingFilter(final NakadiKpiPublisher nakadiKpiPublisher,
                         final AuthorizationService authorizationService,
                         @Value("${nakadi.kpi.event-types.nakadiAccessLog}") final String accessLogEventType) {
        this.nakadiKpiPublisher = nakadiKpiPublisher;
        this.accessLogEventType = accessLogEventType;
        this.authorizationService = authorizationService;
    }

    private class RequestLogInfo {

        private String userAgent;
        private String user;
        private String method;
        private String path;
        private String query;
        private String contentEncoding;
        private Long contentLength;
        private String acceptEncoding;
        private Long requestTime;

        private RequestLogInfo(final HttpServletRequest request, final long requestTime) {
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

    private class AsyncRequestListener implements AsyncListener {
        private final HttpServletResponse response;
        private final String flowId;
        private final RequestLogInfo requestLogInfo;
        private final Span currentSpan;

        private AsyncRequestListener(final HttpServletRequest request, final HttpServletResponse response,
                                     final long startTime, final String flowId, final Span span) {
            this.response = response;
            this.flowId = flowId;
            this.requestLogInfo = new RequestLogInfo(request, startTime);
            this.currentSpan = span;
            logToAccessLog(this.requestLogInfo, HttpStatus.PROCESSING.value(), 0L);
        }

        private void logOnEvent() {
            FlowIdUtils.push(this.flowId);
            logRequest(this.requestLogInfo, this.response.getStatus(), currentSpan);
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
        final long startTime = System.currentTimeMillis();
        final RequestLogInfo requestLogInfo = new RequestLogInfo(request, startTime);
        final RequestType requestType = getRequestType(requestLogInfo);
        if (requestType.equals(RequestType.OTHER)) {
            handleNonTracedRequests(request, response, filterChain, requestLogInfo, startTime);
        } else {
            handleTracedRequests(request, response, filterChain, requestLogInfo, startTime,
                    requestType.getOperationName());
        }
    }

    private void handleNonTracedRequests(final HttpServletRequest request,
                                         final HttpServletResponse response, final FilterChain filterChain,
                                         final RequestLogInfo requestLogInfo, final long startTime)
            throws IOException, ServletException {
        try {
            filterChain.doFilter(request, response);
            if (request.isAsyncStarted()) {
                final String flowId = FlowIdUtils.peek();
                request.getAsyncContext().addListener(new AsyncRequestListener(request, response,
                        startTime, flowId, null));
            }
        } finally {
            if (!request.isAsyncStarted()) {
                logRequest(requestLogInfo, response.getStatus(), null);
            }
        }
    }

    private void handleTracedRequests(final HttpServletRequest request,
                                      final HttpServletResponse response, final FilterChain filterChain,
                                      final RequestLogInfo requestLogInfo, final long startTime,
                                      final String operationName)
            throws IOException, ServletException {
        final Map<String, String> requestHeaders = Collections.list(request.getHeaderNames())
                .stream()
                .collect(Collectors.toMap(h -> h, request::getHeader));
        final SpanContext spanContext = GlobalTracer.get()
                .extract(HTTP_HEADERS, new TextMapExtractAdapter(requestHeaders));
        final Span baseSpan;
        if (spanContext != null) {
            if (operationName.equals(RequestType.COMMIT.getOperationName())) {
                baseSpan = TracingService.getNewSpanWithReference(operationName, startTime, spanContext);
            } else {
                baseSpan = TracingService.getNewSpanWithParent(operationName,
                        startTime, spanContext);
            }
        } else {
            baseSpan = TracingService.getNewSpan(operationName,
                    startTime);
        }
        try {
            final Scope scope = TracingService.activateSpan(baseSpan, false);
            TracingService.setCustomTags(scope.span(),
                    ImmutableMap.<String, Object>builder()
                            .put("client_id", requestLogInfo.user)
                            .put("http.url", requestLogInfo.path + requestLogInfo.query)
                            .put("http.header.content_encoding", requestLogInfo.contentEncoding)
                            .put("http.header.accept_encoding", requestLogInfo.acceptEncoding)
                            .put("http.header.user_agent", requestLogInfo.userAgent)
                            .build());
            request.setAttribute("span", baseSpan);
            //execute request
            filterChain.doFilter(request, response);
            if (request.isAsyncStarted()) {
                final String flowId = FlowIdUtils.peek();
                request.getAsyncContext().addListener(new AsyncRequestListener(request, response, startTime, flowId,
                        baseSpan));
            }
        } finally {
            if (!request.isAsyncStarted()) {
                logRequest(requestLogInfo, response.getStatus(), baseSpan);
            }
            final Map<String, String> spanContextToInject = new HashMap<>();
            GlobalTracer.get().inject(baseSpan.context(),
                    HTTP_HEADERS, new TextMapInjectAdapter(spanContextToInject));
            response.setHeader(SPAN_CONTEXT, spanContextToInject.toString());
            baseSpan.finish();
        }
    }

    private void logRequest(final RequestLogInfo requestLogInfo, final int statusCode, final Span publishingSpan) {
        final Long timeSpentMs = System.currentTimeMillis() - requestLogInfo.requestTime;

        if (!isSuccessPublishingRequest(requestLogInfo, statusCode)) {
            logToAccessLog(requestLogInfo, statusCode, timeSpentMs);
        }
        logToKpiPublisher(requestLogInfo, statusCode, timeSpentMs);
        if (publishingSpan != null) {
            traceRequest(requestLogInfo, statusCode, publishingSpan);
        }
    }

    private void logToKpiPublisher(final RequestLogInfo requestLogInfo, final int statusCode, final Long timeSpentMs) {
        nakadiKpiPublisher.publish(accessLogEventType, () -> new JSONObject()
                .put("method", requestLogInfo.method)
                .put("path", requestLogInfo.path)
                .put("query", requestLogInfo.query)
                .put("app", requestLogInfo.user)
                .put("app_hashed", nakadiKpiPublisher.hash(requestLogInfo.user))
                .put("status_code", statusCode)
                .put("response_time_ms", timeSpentMs));
    }

    private void logToAccessLog(final RequestLogInfo requestLogInfo, final int statusCode, final Long timeSpentMs) {
        ACCESS_LOGGER.info("{} \"{}{}\" \"{}\" \"{}\" {} {}ms \"{}\" \"{}\" {}B",
                requestLogInfo.method,
                requestLogInfo.path,
                requestLogInfo.query,
                requestLogInfo.userAgent,
                requestLogInfo.user,
                statusCode,
                timeSpentMs,
                requestLogInfo.contentEncoding,
                requestLogInfo.acceptEncoding,
                requestLogInfo.contentLength);
    }

    private void traceRequest(final RequestLogInfo requestLogInfo, final int statusCode,
                              final Span span) {
        final RequestType requestType = getRequestType(requestLogInfo);
        if (requestType.equals(RequestType.OTHER)) {
            return;
        }
        final Map<String, Object> tags = new HashMap<String, Object>() {{
            put("http.status_code", statusCode);
            put("content_length", requestLogInfo.contentLength);
        }};

        if (requestType.equals(RequestType.PUBLISHING)) {
            String sloBucket = "5K-50K";
            // contentLength == 0 actually means that contentLength is very big and wasn't reported on time,
            // so we also put it to ">50K" bucket to hack this problem
            if (requestLogInfo.contentLength > 50000 || requestLogInfo.contentLength == 0) {
                sloBucket = ">50K";
            } else if (requestLogInfo.contentLength < 5000) {
                sloBucket = "<5K";
            }
            tags.put("slo_bucket", sloBucket);
            tags.put("error", statusCode == 207 || statusCode >= 500);
        } else {
            tags.put("error", statusCode >= 500);
        }

        final Scope scope = TracingService.activateSpan(span, false);
        TracingService.setCustomTags(scope.span(), tags);
    }

    private boolean isSuccessPublishingRequest(final RequestLogInfo requestLogInfo, final int statusCode) {
        return getRequestType(requestLogInfo).equals(RequestType.PUBLISHING) && statusCode == 200;
    }

    private RequestType getRequestType(final RequestLogInfo requestLogInfo) {
        if (requestLogInfo.path != null && "POST".equals(requestLogInfo.method) &&
                requestLogInfo.path.startsWith("/event-types/") &&
                (requestLogInfo.path.endsWith("/events") || requestLogInfo.path.endsWith("/events/"))) {
            return RequestType.PUBLISHING;
        }
        if (requestLogInfo.path != null
                && ("GET".equals(requestLogInfo.method) || "POST".equals(requestLogInfo.method))
                && requestLogInfo.path.startsWith("/subscriptions/")
                && (requestLogInfo.path.endsWith("/events") || requestLogInfo.path.endsWith("/events/"))) {
            return RequestType.CONSUMPTION;
        }
        if (requestLogInfo.path != null && "POST".equals(requestLogInfo.method) &&
                requestLogInfo.path.startsWith("/subscriptions/") &&
                (requestLogInfo.path.endsWith("/cursors") || requestLogInfo.path.endsWith("/cursors/"))) {
            return RequestType.COMMIT;
        }
        return RequestType.OTHER;
    }
}
