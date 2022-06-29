package org.zalando.nakadi.filters;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.web.filter.OncePerRequestFilter;
import org.zalando.nakadi.domain.Feature;
import org.zalando.nakadi.domain.kpi.AccessLogEvent;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.plugin.api.authz.Subject;
import org.zalando.nakadi.service.FeatureToggleService;
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
    private final AuthorizationService authorizationService;
    private final FeatureToggleService featureToggleService;

    public LoggingFilter(final NakadiKpiPublisher nakadiKpiPublisher,
                         final AuthorizationService authorizationService,
                         final FeatureToggleService featureToggleService) {
        this.nakadiKpiPublisher = nakadiKpiPublisher;
        this.authorizationService = authorizationService;
        this.featureToggleService = featureToggleService;
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
            this.userAgent = Optional.ofNullable(request.getHeader(HttpHeaders.USER_AGENT)).orElse("-");
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

            if (isAccessLogEnabled()) {
                final Long responseLength = 0L;
                final Long timeSpentMs = 0L;
                logToAccessLog(this.requestLogInfo, HttpStatus.PROCESSING.value(), responseLength, timeSpentMs);
            }
        }

        private void logOnEvent() {
            FlowIdUtils.push(this.flowId);
            logRequest(this.requestLogInfo, this.response);
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
                logRequest(requestLogInfo, response);
            }
        }
    }

    private void logRequest(final RequestLogInfo requestLogInfo, final HttpServletResponse response) {
        final Long timeSpentMs = System.currentTimeMillis() - requestLogInfo.requestTime;

        final String responseLengthHeader = response.getHeader(HttpHeaders.CONTENT_LENGTH);
        final Long responseLength = responseLengthHeader == null ? 0L : Long.valueOf(responseLengthHeader);

        final int statusCode = response.getStatus();
        final boolean isServerSideError = statusCode >= 500 || statusCode == 207;

        if (isServerSideError || (isAccessLogEnabled() && !isPublishingRequest(requestLogInfo))) {
            logToAccessLog(requestLogInfo, statusCode, responseLength, timeSpentMs);
        }

        logToKpiPublisher(requestLogInfo, statusCode, responseLength, timeSpentMs);
    }

    private boolean isAccessLogEnabled() {
        return featureToggleService.isFeatureEnabled(Feature.ACCESS_LOG_ENABLED);
    }

    private void logToKpiPublisher(final RequestLogInfo requestLogInfo, final int statusCode, final Long responseLength,
            final Long timeSpentMs) {

        nakadiKpiPublisher.publish(() -> new AccessLogEvent()
                .setMethod(requestLogInfo.method)
                .setPath(requestLogInfo.path)
                .setQuery(requestLogInfo.query)
                .setUserAgent(requestLogInfo.userAgent)
                .setApplicationName(requestLogInfo.user)
                .setHashedApplicationName(nakadiKpiPublisher.hash(requestLogInfo.user))
                .setContentEncoding(requestLogInfo.contentEncoding)
                .setAcceptEncoding(requestLogInfo.acceptEncoding)
                .setStatusCode(statusCode)
                .setTimeSpentMs(timeSpentMs)
                .setRequestLength(requestLogInfo.contentLength)
                .setResponseLength(responseLength));
    }

    private void logToAccessLog(final RequestLogInfo requestLogInfo, final int statusCode, final Long responseLength,
            final Long timeSpentMs) {

        ACCESS_LOGGER.info("{} \"{}{}\" \"{}\" \"{}\" {} {}ms \"{}\" \"{}\" {}B {}B",
                requestLogInfo.method,
                requestLogInfo.path,
                requestLogInfo.query,
                requestLogInfo.userAgent,
                requestLogInfo.user,
                statusCode,
                timeSpentMs,
                requestLogInfo.contentEncoding,
                requestLogInfo.acceptEncoding,
                requestLogInfo.contentLength,
                responseLength);
    }

    private boolean isPublishingRequest(final RequestLogInfo requestLogInfo) {
        return requestLogInfo.path != null && "POST".equals(requestLogInfo.method) &&
                requestLogInfo.path.startsWith("/event-types/") &&
                (requestLogInfo.path.endsWith("/events") || requestLogInfo.path.endsWith("/events/"));
    }
}
