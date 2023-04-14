package org.zalando.nakadi.domain;

import com.fasterxml.jackson.annotation.JsonInclude;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.zalando.nakadi.plugin.api.authz.Resource;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

public class Subscription extends SubscriptionBase {

    public Subscription() {
        super();
    }

    public Subscription(final String id, final DateTime createdAt, final DateTime updatedAt,
                        final SubscriptionBase subscriptionBase) {
        super(subscriptionBase);
        this.id = id;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
    }

    private String id;

    private DateTime createdAt;

    private DateTime updatedAt;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    private List<SubscriptionEventTypeStats> status;

    public DateTime getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(final DateTime updatedAt) {
        this.updatedAt = updatedAt;
    }

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

    public Resource<Subscription> asResource() {
        return new ResourceImpl<>(id, ResourceImpl.SUBSCRIPTION_RESOURCE, getAuthorization(), this);
    }

    @Nullable
    public List<SubscriptionEventTypeStats> getStatus() {
        return status;
    }

    public void setStatus(final List<SubscriptionEventTypeStats> status) {
        this.status = status;
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
        return super.equals(that) && Objects.equals(id, that.id) && Objects.equals(createdAt, that.createdAt);
    }

    public Subscription mergeFrom(final SubscriptionBase newValue) {
        final Subscription subscription = new Subscription(id, createdAt, new DateTime(DateTimeZone.UTC), this);
        subscription.setAuthorization(newValue.getAuthorization());
        subscription.setAnnotations(newValue.getAnnotations());
        subscription.setLabels(newValue.getLabels());
        return subscription;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + id.hashCode();
        result = 31 * result + createdAt.hashCode();
        return result;
    }
}
