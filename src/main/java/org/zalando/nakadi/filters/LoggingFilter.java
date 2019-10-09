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

    private final NakadiKpiPublisher nakadiKpiPublisher;
    private final String accessLogEventType;
    private final AuthorizationService authorizationService;

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
        final Map<String, String> requestHeaders = Collections.list(request.getHeaderNames())
                .stream()
                .collect(Collectors.toMap(h -> h, request::getHeader));
        final SpanContext spanContext = GlobalTracer.get()
                .extract(HTTP_HEADERS, new TextMapExtractAdapter(requestHeaders));
        final Span publishingSpan;
        final long startTime = System.currentTimeMillis();
        if (spanContext != null) {
            publishingSpan = TracingService.getNewSpan("publish_events",
                    startTime, spanContext);
        } else {
            publishingSpan = TracingService.getNewSpan("publish_events",
                    startTime);
        }

        try {
            final RequestLogInfo requestLogInfo = new RequestLogInfo(request, startTime);
            if (isPublishingRequest(requestLogInfo)) {
                final Scope scope = TracingService.activateSpan(publishingSpan, false);
                TracingService.setCustomTags(scope,
                        ImmutableMap.<String, Object>builder()
                                .put("client_id", requestLogInfo.user)
                                .put("http.url", requestLogInfo.path + requestLogInfo.query)
                                .put("http.header.content_encoding", requestLogInfo.contentEncoding)
                                .put("http.header.accept_encoding", requestLogInfo.acceptEncoding)
                                .put("http.header.user_agent", requestLogInfo.userAgent)
                                .build());
                request.setAttribute("span", publishingSpan);
            }
            //execute request
            filterChain.doFilter(request, response);
            if (request.isAsyncStarted()) {
                final String flowId = FlowIdUtils.peek();
                request.getAsyncContext().addListener(new AsyncRequestListener(request, response, startTime, flowId,
                        publishingSpan));
            }
        } finally {
            if (!request.isAsyncStarted()) {
                final RequestLogInfo requestLogInfo = new RequestLogInfo(request, startTime);
                logRequest(requestLogInfo, response.getStatus(), publishingSpan);
            }
            final Map<String, String> spanContextToInject = new HashMap<>();
            GlobalTracer.get().inject(publishingSpan.context(),
                    HTTP_HEADERS, new TextMapInjectAdapter(spanContextToInject));
            response.setHeader("span_ctx",spanContextToInject.toString());
            publishingSpan.finish();
        }
    }

    private void logRequest(final RequestLogInfo requestLogInfo, final int statusCode, final Span publishingSpan) {
        final Long timeSpentMs = System.currentTimeMillis() - requestLogInfo.requestTime;

        if (!isSuccessPublishingRequest(requestLogInfo, statusCode)) {
            logToAccessLog(requestLogInfo, statusCode, timeSpentMs);
        }
        logToNakadi(requestLogInfo, statusCode, timeSpentMs);
        traceRequest(requestLogInfo, statusCode, timeSpentMs, publishingSpan);
    }

    private void logToNakadi(final RequestLogInfo requestLogInfo, final int statusCode, final Long timeSpentMs) {
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

    private void traceRequest(final RequestLogInfo requestLogInfo, final int statusCode, final Long timeSpentMs,
                              final Span publishingSpan) {
        if (!isPublishingRequest(requestLogInfo)) {
            return;
        }
        final Scope scope = TracingService.activateSpan(publishingSpan, false);
        String sloBucket = "5K-50K";
        // contentLength == 0 actually means that contentLength is very big and wasn't reported on time,
        // so we also put it to ">50K" bucket to hack this problem
        if (requestLogInfo.contentLength > 50000 || requestLogInfo.contentLength == 0) {
            sloBucket = ">50K";
        } else if (requestLogInfo.contentLength < 5000) {
            sloBucket = "<5K";
        }
        final String eventType = requestLogInfo.path.substring("/event-types/".length(),
                requestLogInfo.path.lastIndexOf("/events"));
        TracingService.setCustomTags(scope, ImmutableMap.<String, Object>builder()
                .put("event_type", eventType)
                .put("http.status_code", statusCode)
                .put("slo_bucket", sloBucket)
                .put("content_length", requestLogInfo.contentLength).build());
    }

    private boolean isSuccessPublishingRequest(final RequestLogInfo requestLogInfo, final int statusCode) {
        return isPublishingRequest(requestLogInfo) && statusCode == 200;
    }

    private boolean isPublishingRequest(final RequestLogInfo requestLogInfo) {
        return requestLogInfo.path != null && "POST".equals(requestLogInfo.method) &&
                requestLogInfo.path.startsWith("/event-types/") &&
                (requestLogInfo.path.endsWith("/events") || requestLogInfo.path.endsWith("/events/"));
    }
}
