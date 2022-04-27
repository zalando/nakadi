package org.zalando.nakadi.domain.kpi;

import org.zalando.nakadi.config.KPIEventTypes;

public class SubscriptionLogEvent extends KPIEvent {

    @KPIField("subscription_id")
    protected String subscriptionId;
    @KPIField("status")
    protected String status;

    public SubscriptionLogEvent() {
        super(KPIEventTypes.SUBSCRIPTION_LOG);
    }

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
