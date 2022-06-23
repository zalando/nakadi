package org.zalando.nakadi.filters;

import com.google.common.io.CountingInputStream;
import com.google.common.io.CountingOutputStream;
import com.google.common.net.HttpHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import javax.servlet.ReadListener;
import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.ServletOutputStream;
import javax.servlet.WriteListener;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpServletResponseWrapper;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.OutputStream;
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

        private final RequestWrapper requestWrapper;
        private final ResponseWrapper responseWrapper;

        private final String userAgent;
        private final String user;
        private final String method;
        private final String path;
        private final String query;
        private final String contentEncoding;
        private final String acceptEncoding;
        private final long requestStartedAt;

        private RequestLogInfo(final RequestWrapper requestWrapper, final ResponseWrapper responseWrapper,
                final long requestStartedAt) {

            this.requestWrapper = requestWrapper;
            this.responseWrapper = responseWrapper;

            this.userAgent = Optional.ofNullable(requestWrapper.getHeader(HttpHeaders.USER_AGENT)).orElse("-");
            this.user = authorizationService.getSubject().map(Subject::getName).orElse("-");
            this.method = requestWrapper.getMethod();
            this.path = requestWrapper.getRequestURI();
            this.query = Optional.ofNullable(requestWrapper.getQueryString()).map(q -> "?" + q).orElse("");
            this.contentEncoding = Optional.ofNullable(
                    requestWrapper.getHeader(HttpHeaders.CONTENT_ENCODING)).orElse("-");
            this.acceptEncoding = Optional.ofNullable(
                    requestWrapper.getHeader(HttpHeaders.ACCEPT_ENCODING)).orElse("-");

            this.requestStartedAt = requestStartedAt;
        }

        private long getRequestLength() {
            return requestWrapper.getInputStreamBytesCount();
        }

        private long getResponseLength() {
            return responseWrapper.getOutputStreamBytesCount();
        }

        private int getResponseStatus() {
            return responseWrapper.getStatus();
        }
    }

    private class AsyncRequestListener implements AsyncListener {
        private final RequestLogInfo requestLogInfo;
        private final String flowId;

        private AsyncRequestListener(final RequestLogInfo requestLogInfo, final String flowId) {

            this.requestLogInfo = requestLogInfo;
            this.flowId = flowId;

            if (isAccessLogEnabled()) {
                final Long timeSpentMs = 0L;
                logToAccessLog(this.requestLogInfo, HttpStatus.PROCESSING.value(), timeSpentMs);
            }
        }

        private void logOnEvent() {
            FlowIdUtils.push(this.flowId);
            logRequest(this.requestLogInfo);
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
        final RequestWrapper requestWrapper = new RequestWrapper(request);
        final ResponseWrapper responseWrapper = new ResponseWrapper(response);
        final RequestLogInfo requestLogInfo = new RequestLogInfo(requestWrapper, responseWrapper, startTime);
        try {
            filterChain.doFilter(requestWrapper, responseWrapper);
            if (request.isAsyncStarted()) {
                final String flowId = FlowIdUtils.peek();
                request.getAsyncContext().addListener(new AsyncRequestListener(requestLogInfo, flowId));
            }
        } finally {
            if (!request.isAsyncStarted()) {
                logRequest(requestLogInfo);
            }
        }
    }

    private void logRequest(final RequestLogInfo requestLogInfo) {
        final Long timeSpentMs = System.currentTimeMillis() - requestLogInfo.requestStartedAt;

        final int statusCode = requestLogInfo.getResponseStatus();
        final boolean isServerSideError = statusCode >= 500 || statusCode == 207;

        if (isServerSideError || (isAccessLogEnabled() && !isPublishingRequest(requestLogInfo))) {
            logToAccessLog(requestLogInfo, statusCode, timeSpentMs);
        }

        logToKpiPublisher(requestLogInfo, statusCode, timeSpentMs);
    }

    private boolean isAccessLogEnabled() {
        return featureToggleService.isFeatureEnabled(Feature.ACCESS_LOG_ENABLED);
    }

    private void logToKpiPublisher(final RequestLogInfo requestLogInfo, final int statusCode, final Long timeSpentMs) {

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
                .setRequestLength(requestLogInfo.getRequestLength())
                .setResponseLength(requestLogInfo.getResponseLength()));
    }

    private void logToAccessLog(final RequestLogInfo requestLogInfo, final int statusCode, final Long timeSpentMs) {

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
                requestLogInfo.getRequestLength(),
                requestLogInfo.getResponseLength());
    }

    private boolean isPublishingRequest(final RequestLogInfo requestLogInfo) {
        return requestLogInfo.path != null && "POST".equals(requestLogInfo.method) &&
                requestLogInfo.path.startsWith("/event-types/") &&
                (requestLogInfo.path.endsWith("/events") || requestLogInfo.path.endsWith("/events/"));
    }

    // ====================================================================================================
    private static class RequestWrapper extends HttpServletRequestWrapper {

        private ServletInputStreamWrapper inputStream;
        private BufferedReader reader;

        RequestWrapper(final HttpServletRequest request) {
            super(request);
        }

        long getInputStreamBytesCount() {
            return inputStream != null ? inputStream.getCount() : 0;
        }

        @Override
        public ServletInputStream getInputStream() throws IOException {
            if (inputStream == null) {
                inputStream = new ServletInputStreamWrapper(super.getInputStream());
            }
            return inputStream;
        }

        @Override
        public BufferedReader getReader() throws IOException {
            if (reader == null) {
                reader = new BufferedReader(new InputStreamReader(getInputStream()));
            }
            return reader;
        }
    }

    private static class ServletInputStreamWrapper extends ServletInputStream {

        private final CountingInputStream inputStream;

        ServletInputStreamWrapper(final InputStream inputStream) {
            this.inputStream = new CountingInputStream(inputStream);
        }

        long getCount() {
            return inputStream.getCount();
        }

        @Override
        public int read() throws IOException {
            return inputStream.read();
        }

        @Override
        public void close() throws IOException {
            inputStream.close();
        }

        @Override
        public boolean isFinished() {
            try {
                return inputStream.available() == 0;
            } catch (final IOException e) {
                // TODO
                //LOG.error("Error occurred when reading request input stream", e);
                return false;
            }
        }

        @Override
        public boolean isReady() {
            return true;
        }

        @Override
        public void setReadListener(final ReadListener listener) {
            throw new UnsupportedOperationException("Not supported");
        }
    }

    // ====================================================================================================
    private static class ResponseWrapper extends HttpServletResponseWrapper {

        private ServletOutputStreamWrapper outputStream;
        private PrintWriter writer;

        ResponseWrapper(final HttpServletResponse response) {
            super(response);
        }

        long getOutputStreamBytesCount() {
            return outputStream != null ? outputStream.getCount() : 0;
        }

        @Override
        public ServletOutputStream getOutputStream() throws IOException {
            if (outputStream == null) {
                outputStream = new ServletOutputStreamWrapper(super.getOutputStream());
            }
            return outputStream;
        }

        @Override
        public PrintWriter getWriter() throws IOException {
            if (writer == null) {
                writer = new PrintWriter(getOutputStream());
            }
            return writer;
        }
    }

    private static class ServletOutputStreamWrapper extends ServletOutputStream {

        private final CountingOutputStream outputStream;

        ServletOutputStreamWrapper(final OutputStream outputStream) {
            this.outputStream = new CountingOutputStream(outputStream);
        }

        long getCount() {
            return outputStream.getCount();
        }

        @Override
        public void write(final int b) throws IOException {
            outputStream.write(b);
        }

        @Override
        public void close() throws IOException {
            outputStream.close();
        }

        @Override
        public boolean isReady() {
            return true;
        }

        @Override
        public void setWriteListener(final WriteListener listener) {
            throw new UnsupportedOperationException("Not supported");
        }
    }
}
