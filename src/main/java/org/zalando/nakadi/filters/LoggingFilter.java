package org.zalando.nakadi.filters;

import com.google.common.net.HttpHeaders;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;
import org.zalando.nakadi.service.NakadiKpiPublisher;

import javax.servlet.AsyncEvent;
import javax.servlet.AsyncListener;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.security.Principal;
import java.util.Optional;

@Component
public class LoggingFilter extends OncePerRequestFilter {

    // We are using empty log name, cause it is used only for access log and we do not care about class name
    private static final Logger ACCESS_LOGGER = LoggerFactory.getLogger("ACCESS_LOG");

    private final NakadiKpiPublisher nakadiKpiPublisher;
    private final String accessLogEventType;

    @Autowired
    public LoggingFilter(final NakadiKpiPublisher nakadiKpiPublisher,
                         @Value("${nakadi.kpi.event-types.nakadiAccessLog}") final String accessLogEventType) {
        this.nakadiKpiPublisher = nakadiKpiPublisher;
        this.accessLogEventType = accessLogEventType;
    }

    public class AsyncRequestListener implements AsyncListener {
        private final HttpServletRequest request;
        private final HttpServletResponse response;
        private long startTime;

        public AsyncRequestListener(final HttpServletRequest request, final HttpServletResponse response,
                                    final long startTime) {
            this.request = request;
            this.response = response;
            this.startTime = startTime;
        }
        @Override
        public void onComplete(final AsyncEvent event) {
            writeToAccessLogAndEventType(request, response, startTime);
        }

        @Override
        public void onTimeout(final AsyncEvent event) {
            writeToAccessLogAndEventType(request, response, startTime);
        }

        @Override
        public void onError(final AsyncEvent event) {
            writeToAccessLogAndEventType(request, response, startTime);
        }

        @Override
        public void onStartAsync(final AsyncEvent event) {
        }
    }

    @Override
    protected void doFilterInternal(final HttpServletRequest request,
                                    final HttpServletResponse response, final FilterChain filterChain)
            throws ServletException, IOException {
        final long start = System.currentTimeMillis();
        boolean willBeLogged = false;
        try {
            //execute request
            filterChain.doFilter(request, response);
            if (request.isAsyncStarted()) {
                request.getAsyncContext().addListener(new AsyncRequestListener(request, response, start));
                willBeLogged = true;
            }
        } finally {
            if(!willBeLogged) {
                writeToAccessLogAndEventType(request, response, start);
            }
        }
    }

    private void writeToAccessLogAndEventType(final HttpServletRequest request, final HttpServletResponse response,
                                             final long requestTime) {
            final long currentTime = System.currentTimeMillis();
            final Long timing = currentTime - requestTime;
            final String userAgent = Optional.ofNullable(request.getHeader("User-Agent")).orElse("-");
            final String user = Optional.ofNullable(request.getUserPrincipal()).map(Principal::getName).orElse("-");
            final String method = request.getMethod();
            final String path = request.getRequestURI();
            final String query = Optional.ofNullable(request.getQueryString()).map(q -> "?" + q).orElse("");
            final String contentEncoding = Optional.ofNullable(request.getHeader(HttpHeaders.CONTENT_ENCODING))
                    .orElse("-");
            final String acceptEncoding = Optional.ofNullable(request.getHeader(HttpHeaders.ACCEPT_ENCODING))
                    .orElse("-");
            final Long contentLength = request.getContentLengthLong() == -1 ? 0 : request.getContentLengthLong();

            ACCESS_LOGGER.info("{} \"{}{}\" \"{}\" \"{}\" {} {}ms \"{}\" \"{}\" {}B",
                    method,
                    path,
                    query,
                    userAgent,
                    user,
                    response.getStatus(),
                    timing,
                    contentEncoding,
                    acceptEncoding,
                    contentLength);
            nakadiKpiPublisher.publish(accessLogEventType, () -> new JSONObject()
                    .put("method", method)
                    .put("path", path)
                    .put("query", query)
                    .put("app", user)
                    .put("app_hashed", nakadiKpiPublisher.hash(user))
                    .put("status_code", response.getStatus())
                    .put("response_time_ms", timing));
    }
}
