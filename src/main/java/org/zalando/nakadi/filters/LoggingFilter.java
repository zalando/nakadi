package org.zalando.nakadi.filters;

import com.google.common.net.HttpHeaders;
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
import org.zalando.nakadi.util.FlowIdUtils;

import javax.servlet.AsyncEvent;
import javax.servlet.AsyncListener;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

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

        private AsyncRequestListener(final HttpServletRequest request, final HttpServletResponse response,
                                     final long startTime, final String flowId) {
            this.response = response;
            this.flowId = flowId;

            this.requestLogInfo = new RequestLogInfo(request, startTime);
            logToAccessLog(this.requestLogInfo, HttpStatus.PROCESSING.value(), 0L);
        }

        private void logOnEvent() {
            FlowIdUtils.push(this.flowId);
            logRequest(this.requestLogInfo, this.response.getStatus());
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
        final long start = System.currentTimeMillis();
        try {
            //execute request
            filterChain.doFilter(request, response);
            if (request.isAsyncStarted()) {
                final String flowId = FlowIdUtils.peek();
                request.getAsyncContext().addListener(new AsyncRequestListener(request, response, start, flowId));
            }
        } finally {
            if (!request.isAsyncStarted()) {
                final RequestLogInfo requestLogInfo = new RequestLogInfo(request, start);
                logRequest(requestLogInfo, response.getStatus());
            }
        }
    }

    private void logRequest(final RequestLogInfo requestLogInfo, final int statusCode) {
        final Long timeSpentMs = System.currentTimeMillis() - requestLogInfo.requestTime;

        logToAccessLog(requestLogInfo, statusCode, timeSpentMs);
        logToNakadi(requestLogInfo, statusCode, timeSpentMs);
        traceRequest(requestLogInfo, statusCode, timeSpentMs);
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

    private void traceRequest(final RequestLogInfo requestLogInfo, final int statusCode, final Long timeSpentMs) {
        if (requestLogInfo.path != null && "POST".equals(requestLogInfo.method) &&
                requestLogInfo.path.startsWith("/event-types/") && requestLogInfo.path.contains("/events")) {

            final String eventType = requestLogInfo.path.substring("/event-types/".length(),
                    requestLogInfo.path.lastIndexOf("/events"));

            String sloBucket = "5K-50K";
            if (requestLogInfo.contentLength < 5000) {
                sloBucket = "<5K";
            } else if (requestLogInfo.contentLength > 50000) {
                sloBucket = ">50K";
            }

            GlobalTracer.get()
                    .buildSpan("publish_events")
                    .withStartTimestamp(TimeUnit.MILLISECONDS.toMicros(requestLogInfo.requestTime))
                    .start()
                    .setTag("client_id", requestLogInfo.user)
                    .setTag("event_type", eventType)
                    .setTag("error", statusCode == 207 || statusCode >= 500)
                    .setTag("http.status_code", statusCode)
                    .setTag("http.url", requestLogInfo.path + requestLogInfo.query)
                    .setTag("http.header.content_encoding", requestLogInfo.contentEncoding)
                    .setTag("http.header.accept_encoding", requestLogInfo.acceptEncoding)
                    .setTag("http.header.user_agent", requestLogInfo.userAgent)
                    .setTag("slo_bucket", sloBucket)
                    .setTag("content_length", requestLogInfo.contentLength)
                    .finish(TimeUnit.MILLISECONDS.toMicros(requestLogInfo.requestTime + timeSpentMs));
        }
    }
}
