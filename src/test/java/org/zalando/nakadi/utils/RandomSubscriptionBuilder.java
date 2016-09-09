package org.zalando.nakadi.utils;

import com.google.common.collect.ImmutableSet;
import org.joda.time.DateTime;
import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.domain.SubscriptionBase;

import java.util.Set;

import static org.zalando.nakadi.domain.SubscriptionBase.InitialPosition.END;
import static org.zalando.nakadi.utils.TestUtils.randomDate;
import static org.zalando.nakadi.utils.TestUtils.randomTextString;
import static org.zalando.nakadi.utils.TestUtils.randomUUID;

public class RandomSubscriptionBuilder {

    private String owningApplication;
    private Set<String> eventTypes;
    private String consumerGroup;
    private SubscriptionBase.InitialPosition startFrom;
    private String id;
    private DateTime createdAt;

    protected RandomSubscriptionBuilder() {
        id = randomUUID();
        startFrom = END;
        createdAt = randomDate();
        owningApplication = randomTextString();
        consumerGroup = randomTextString();
        eventTypes = ImmutableSet.of(randomTextString());
    }

    public static RandomSubscriptionBuilder builder() {
        return new RandomSubscriptionBuilder();
    }

    public RandomSubscriptionBuilder withId(final String id) {
        this.id = id;
        return this;
    }

    public RandomSubscriptionBuilder withCreatedAt(final DateTime createdAt) {
        this.createdAt = createdAt;
        return this;
    }

    public RandomSubscriptionBuilder withOwningApplication(final String owningApplication) {
        this.owningApplication = owningApplication;
        return this;
    }

    public RandomSubscriptionBuilder withEventType(final String eventType) {
        this.eventTypes = ImmutableSet.of(eventType);
        return this;
    }

    public RandomSubscriptionBuilder withEventTypes(final Set<String> eventTypes) {
        this.eventTypes = eventTypes;
        return this;
    }

    public RandomSubscriptionBuilder withConsumerGroup(final String consumerGroup) {
        this.consumerGroup = consumerGroup;
        return this;
    }

    public RandomSubscriptionBuilder withStartFrom(final SubscriptionBase.InitialPosition startFrom) {
        this.startFrom = startFrom;
        return this;
    }

    public Subscription build() {
        final Subscription subscription = new Subscription();
        subscription.setId(id);
        subscription.setCreatedAt(createdAt);
        subscription.setOwningApplication(owningApplication);
        subscription.setEventTypes(eventTypes);
        subscription.setConsumerGroup(consumerGroup);
        subscription.setStartFrom(startFrom);
        return subscription;
    }

    public SubscriptionBase buildSubscriptionBase() {
        final SubscriptionBase subscriptionBase = new SubscriptionBase();
        subscriptionBase.setOwningApplication(owningApplication);
        subscriptionBase.setEventTypes(eventTypes);
        subscriptionBase.setConsumerGroup(consumerGroup);
        subscriptionBase.setStartFrom(startFrom);
        return subscriptionBase;
    }

}
