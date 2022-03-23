package org.zalando.nakadi.domain.kpi;


public class AccessLogEvent {
    private String method;
    private String path;
    private String query;
    private String user;
    private String contentEncoding;
    private String acceptEncoding;
    private int statusCode;
    private long timeSpentMs;

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

    public String getUser() {
        return user;
    }

    public AccessLogEvent setUser(final String user) {
        this.user = user;
        return this;
    }

    public String getContentEncoding() {
        return contentEncoding;
    }

    public AccessLogEvent setContentEncoding(final String contentEncoding) {
        this.contentEncoding = contentEncoding;
        return this;
    }

    public String getAcceptEncoding() {
        return acceptEncoding;
    }

    public AccessLogEvent setAcceptEncoding(final String acceptEncoding) {
        this.acceptEncoding = acceptEncoding;
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
}
