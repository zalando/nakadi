package org.zalando.nakadi.domain.kpi;

import org.zalando.nakadi.config.KPIEventTypes;

public class AccessLogEvent extends KPIEvent {
    @KPIField("method")
    private String method;
    @KPIField("path")
    private String path;
    @KPIField("query")
    private String query;
    @KPIField("user_agent")
    private String userAgent;
    @KPIField("app")
    private String applicationName;
    @KPIField("app_hashed")
    private String hashedApplicationName;
    @KPIField("status_code")
    private int statusCode;
    @KPIField("response_time_ms")
    private long timeSpentMs;
    @KPIField("accept_encoding")
    private String acceptEncoding;
    @KPIField("content_encoding")
    private String contentEncoding;
    @KPIField("request_length")
    private long requestLength;
    @KPIField("response_length")
    private long responseLength;

    public AccessLogEvent() {
        super(KPIEventTypes.ACCESS_LOG, "1");
    }

    public String getMethod() {
        return method;
    }

    public AccessLogEvent setMethod(final String method) {
        this.method = method;
        return this;
    }

    public String getPath() {
        return path;
    }

    public AccessLogEvent setPath(final String path) {
        this.path = path;
        return this;
    }

    public String getQuery() {
        return query;
    }

    public AccessLogEvent setQuery(final String query) {
        this.query = query;
        return this;
    }

    public String getUserAgent() {
        return userAgent;
    }

    public AccessLogEvent setUserAgent(final String userAgent) {
        this.userAgent = userAgent;
        return this;
    }

    public String getApplicationName() {
        return applicationName;
    }

    public AccessLogEvent setApplicationName(final String applicationName) {
        this.applicationName = applicationName;
        return this;
    }

    public String getHashedApplicationName() {
        return hashedApplicationName;
    }

    public AccessLogEvent setHashedApplicationName(final String hashedApplicationName) {
        this.hashedApplicationName = hashedApplicationName;
        return this;
    }

    public int getStatusCode() {
        return statusCode;
    }

    public AccessLogEvent setStatusCode(final int statusCode) {
        this.statusCode = statusCode;
        return this;
    }

    public long getTimeSpentMs() {
        return timeSpentMs;
    }

    public AccessLogEvent setTimeSpentMs(final long timeSpentMs) {
        this.timeSpentMs = timeSpentMs;
        return this;
    }

    public String getAcceptEncoding() {
        return acceptEncoding;
    }

    public AccessLogEvent setAcceptEncoding(final String acceptEncoding) {
        this.acceptEncoding = acceptEncoding;
        return this;
    }

    public String getContentEncoding() {
        return contentEncoding;
    }

    public AccessLogEvent setContentEncoding(final String contentEncoding) {
        this.contentEncoding = contentEncoding;
        return this;
    }

    public long getRequestLength() {
        return requestLength;
    }

    public AccessLogEvent setRequestLength(final long requestLength) {
        this.requestLength = requestLength;
        return this;
    }

    public long getResponseLength() {
        return responseLength;
    }

    public AccessLogEvent setResponseLength(final long responseLength) {
        this.responseLength = responseLength;
        return this;
    }
}
