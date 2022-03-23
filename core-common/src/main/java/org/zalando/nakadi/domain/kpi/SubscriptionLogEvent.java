package org.zalando.nakadi.domain.kpi;

public class SubscriptionLogEvent {
    private String subscriptionId;
    private String status;

    public String getSubscriptionId() {
        return subscriptionId;
    }

    public SubscriptionLogEvent setSubscriptionId(final String subscriptionId) {
        this.subscriptionId = subscriptionId;
        return this;
    }

    public String getStatus() {
        return status;
    }

    public SubscriptionLogEvent setStatus(final String status) {
        this.status = status;
        return this;
    }
}
