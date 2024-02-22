package org.zalando.nakadi.plugin.auth;

import javax.servlet.http.HttpServletRequest;

public class XConsumer {
    private final String content;
    private final String keyId;
    private final String signature;

    public XConsumer(final String content, final String keyId, final String signature) {
        this.content = content;
        this.keyId = keyId;
        this.signature = signature;
    }

    public static XConsumer fromRequest(final HttpServletRequest request) {
        return new XConsumer(
                request.getHeader("X-Consumer"),
                request.getHeader("X-Consumer-Key-ID"),
                request.getHeader("X-Consumer-Signature"));
    }

    public String getContent() {
        return content;
    }

    public boolean isPresent() {
        return content != null;
    }

    public String getSignature() {
        return signature;
    }

    public String getKeyId() {
        return keyId;
    }
}
