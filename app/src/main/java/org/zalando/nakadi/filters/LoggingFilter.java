package org.zalando.nakadi.filters;

import com.google.common.net.HttpHeaders;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.web.filter.OncePerRequestFilter;
import org.zalando.nakadi.domain.Feature;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.plugin.api.authz.Subject;
import org.zalando.nakadi.service.FeatureToggleService;
import org.zalando.nakadi.service.publishing.AvroEventPublisher;
import org.zalando.nakadi.service.publishing.NakadiKpiPublisher;
import org.zalando.nakadi.util.FlowIdUtils;

import javax.servlet.AsyncEvent;
import javax.servlet.AsyncListener;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Optional;

public class LoggingFilter extends OncePerRequestFilter {

    // We are using empty log name, cause it is used only for access log and we do not care about class name
    private static final Logger ACCESS_LOGGER = LoggerFactory.getLogger("ACCESS_LOG");
    private final NakadiKpiPublisher nakadiKpiPublisher;
    private final String accessLogEventType;
    private final AuthorizationService authorizationService;
    private final FeatureToggleService featureToggleService;
    private final AvroEventPublisher avroEventPublisher;

    public LoggingFilter(final NakadiKpiPublisher nakadiKpiPublisher,
                         final AuthorizationService authorizationService,
                         final FeatureToggleService featureToggleService,
                         final String accessLogEventType,
                         final AvroEventPublisher avroEventPublisher) {
        this.nakadiKpiPublisher = nakadiKpiPublisher;
        this.accessLogEventType = accessLogEventType;
        this.authorizationService = authorizationService;
        this.featureToggleService = featureToggleService;
        this.avroEventPublisher = avroEventPublisher;
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

            if (featureToggleService.isFeatureEnabled(Feature.ACCESS_LOG_ENABLED)) {
                logToAccessLog(this.requestLogInfo, HttpStatus.PROCESSING.value(), 0L);
            }
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
        final long startTime = System.currentTimeMillis();
        final RequestLogInfo requestLogInfo = new RequestLogInfo(request, startTime);
        try {
            filterChain.doFilter(request, response);
            if (request.isAsyncStarted()) {
                final String flowId = FlowIdUtils.peek();
                request.getAsyncContext().addListener(new AsyncRequestListener(request, response,
                        startTime, flowId));
            }
        } finally {
            if (!request.isAsyncStarted()) {
                logRequest(requestLogInfo, response.getStatus());
            }
        }
    }

    private void logRequest(final RequestLogInfo requestLogInfo, final int statusCode) {
        final Long timeSpentMs = System.currentTimeMillis() - requestLogInfo.requestTime;

        final boolean isServerSideError = statusCode >= 500 || statusCode == 207;

        if (isServerSideError || (isAccessLogEnabled() && !isPublishingRequest(requestLogInfo))) {
            logToAccessLog(requestLogInfo, statusCode, timeSpentMs);
        }

        logToNakadi(requestLogInfo, statusCode, timeSpentMs);
    }

    private boolean isAccessLogEnabled() {
        return featureToggleService.isFeatureEnabled(Feature.ACCESS_LOG_ENABLED);
    }

    private void logToNakadi(final RequestLogInfo requestLogInfo, final int statusCode, final Long timeSpentMs) {
        if (featureToggleService.isFeatureEnabled(Feature.ACCESS_LOG_IN_AVRO)) {
            avroEventPublisher.publishAvro(accessLogEventType,
                    requestLogInfo.method,
                    requestLogInfo.path,
                    requestLogInfo.query,
                    requestLogInfo.user,
                    nakadiKpiPublisher.hash(requestLogInfo.user),
                    statusCode,
                    timeSpentMs);
        } else {
            nakadiKpiPublisher.publish(accessLogEventType, () -> new JSONObject()
                    .put("method", requestLogInfo.method)
                    .put("path", requestLogInfo.path)
                    .put("query", requestLogInfo.query)
                    .put("app", requestLogInfo.user)
                    .put("app_hashed", nakadiKpiPublisher.hash(requestLogInfo.user))
                    .put("status_code", statusCode)
                    .put("response_time_ms", timeSpentMs));
        }
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

    private boolean isPublishingRequest(final RequestLogInfo requestLogInfo) {
        return requestLogInfo.path != null && "POST".equals(requestLogInfo.method) &&
                requestLogInfo.path.startsWith("/event-types/") &&
                (requestLogInfo.path.endsWith("/events") || requestLogInfo.path.endsWith("/events/"));
    }
}
