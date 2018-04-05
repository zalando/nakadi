package org.zalando.nakadi.domain;

import org.joda.time.DateTime;

import java.util.List;

public class Subscription extends SubscriptionBase {

    public Subscription() {
        super();
    }

    public Subscription(final String id, final DateTime createdAt, final SubscriptionBase subscriptionBase) {
        super(subscriptionBase);
        this.id = id;
        this.createdAt = createdAt;
    }

    private String id;

    private DateTime createdAt;

    private List<SubscriptionEventTypeStats> stats;

    public String getId() {
        return id;
    }

    public void setId(final String id) {
        this.id = id;
    }

    public DateTime getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(final DateTime createdAt) {
        this.createdAt = createdAt;
    }

    public List<SubscriptionEventTypeStats> getStats() {
        return stats;
    }

    public void setStats(final List<SubscriptionEventTypeStats> stats) {
        this.stats = stats;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final Subscription that = (Subscription) o;
        return super.equals(that) && id.equals(that.id) && createdAt.equals(that.createdAt);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + id.hashCode();
        result = 31 * result + createdAt.hashCode();
        return result;
    }
}
