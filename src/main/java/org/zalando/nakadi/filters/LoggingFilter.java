package org.zalando.nakadi.filters;

import com.google.common.net.HttpHeaders;
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
            this.user = Optional.ofNullable(authorizationService.getSubject()).map(Subject::getName)
                    .orElse("-");
            this.method = request.getMethod();
            this.path = request.getRequestURI();
            this.query = Optional.ofNullable(request.getQueryString()).map(q -> "?" + q).orElse("");
            this.contentEncoding = Optional.ofNullable(request.getHeader(HttpHeaders.CONTENT_ENCODING))
                .orElse("-");
            this.acceptEncoding = Optional.ofNullable(request.getHeader(HttpHeaders.ACCEPT_ENCODING))
                .orElse("-");
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
            ACCESS_LOGGER.info("{} \"{}{}\" \"{}\" \"{}\" {} {}ms \"{}\" \"{}\" {}B",
                    requestLogInfo.method,
                    requestLogInfo.path,
                    requestLogInfo.query,
                    requestLogInfo.userAgent,
                    requestLogInfo.user,
                    HttpStatus.PROCESSING.value(),
                    0,
                    requestLogInfo.contentEncoding,
                    requestLogInfo.acceptEncoding,
                    requestLogInfo.contentLength);
        }

        private void logOnEvent() {
            FlowIdUtils.push(this.flowId);
            writeToAccessLogAndEventType(this.requestLogInfo, this.response);
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
            throws IOException, ServletException{
        final long start = System.currentTimeMillis();
        try {
            //execute request
            filterChain.doFilter(request, response);
            if (request.isAsyncStarted()) {
                final String flowId = FlowIdUtils.peek();
                request.getAsyncContext().addListener(new AsyncRequestListener(request, response, start, flowId));
            }
        } finally {
            if(!request.isAsyncStarted()) {
                final RequestLogInfo requestLogInfo = new RequestLogInfo(request, start);
                writeToAccessLogAndEventType(requestLogInfo, response);
            }
        }
    }

    private void writeToAccessLogAndEventType(final RequestLogInfo requestLogInfo, final HttpServletResponse response) {
            final long currentTime = System.currentTimeMillis();
            final Long timing = currentTime - requestLogInfo.requestTime;

            ACCESS_LOGGER.info("{} \"{}{}\" \"{}\" \"{}\" {} {}ms \"{}\" \"{}\" {}B",
                    requestLogInfo.method,
                    requestLogInfo.path,
                    requestLogInfo.query,
                    requestLogInfo.userAgent,
                    requestLogInfo.user,
                    response.getStatus(),
                    timing,
                    requestLogInfo.contentEncoding,
                    requestLogInfo.acceptEncoding,
                    requestLogInfo.contentLength);
            nakadiKpiPublisher.publish(accessLogEventType, () -> new JSONObject()
                    .put("method", requestLogInfo.method)
                    .put("path", requestLogInfo.path)
                    .put("query", requestLogInfo.query)
                    .put("app", requestLogInfo.user)
                    .put("app_hashed", nakadiKpiPublisher.hash(requestLogInfo.user))
                    .put("status_code", response.getStatus())
                    .put("response_time_ms", timing));
    }
}
