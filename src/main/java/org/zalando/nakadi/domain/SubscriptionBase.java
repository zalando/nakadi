package org.zalando.nakadi.domain;

import com.google.common.collect.ImmutableList;
import org.zalando.nakadi.view.SubscriptionCursorWithoutToken;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.Sets.newTreeSet;

public class SubscriptionBase {

    public enum InitialPosition {
        BEGIN,
        END,
        CURSORS
    }

    @NotNull
    @Size(min = 1, message = "must contain at least one character")
    private String owningApplication;

    @NotNull
    @Size(min = 1, message = "must contain at least one element")
    private Set<String> eventTypes;

    @NotNull
    @Size(min = 1, message = "must contain at least one character")
    private String consumerGroup = "default";

    @NotNull
    private InitialPosition readFrom = InitialPosition.END;

    private List<SubscriptionCursorWithoutToken> initialCursors = ImmutableList.of();

    public SubscriptionBase() {
    }

    public SubscriptionBase(final SubscriptionBase subscriptionBase) {
        this.setOwningApplication(subscriptionBase.getOwningApplication());
        this.setEventTypes(subscriptionBase.getEventTypes());
        this.setConsumerGroup(subscriptionBase.getConsumerGroup());
        this.setReadFrom(subscriptionBase.getReadFrom());
        this.setInitialCursors(subscriptionBase.getInitialCursors());
    }

    public String getOwningApplication() {
        return owningApplication;
    }

    public void setOwningApplication(final String owningApplication) {
        this.owningApplication = owningApplication;
    }

    public Set<String> getEventTypes() {
        return Collections.unmodifiableSet(eventTypes);
    }

    public void setEventTypes(final Set<String> eventTypes) {
        this.eventTypes = newTreeSet(eventTypes);
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(final String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public InitialPosition getReadFrom() {
        return readFrom;
    }

    public void setReadFrom(final InitialPosition readFrom) {
        this.readFrom = readFrom;
    }

    public List<SubscriptionCursorWithoutToken> getInitialCursors() {
        return Collections.unmodifiableList(initialCursors);
    }

    public void setInitialCursors(@Nullable final List<SubscriptionCursorWithoutToken> initialCursors) {
        this.initialCursors = Optional.ofNullable(initialCursors).orElse(ImmutableList.of());
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final SubscriptionBase that = (SubscriptionBase) o;
        return owningApplication.equals(that.owningApplication)
                && eventTypes.equals(that.eventTypes)
                && consumerGroup.equals(that.consumerGroup)
                && readFrom.equals(that.readFrom)
                && initialCursors.equals(that.initialCursors);
    }

    @Override
    public int hashCode() {
        int result = owningApplication != null ? owningApplication.hashCode() : 0;
        result = 31 * result + (eventTypes != null ? eventTypes.hashCode() : 0);
        result = 31 * result + (consumerGroup != null ? consumerGroup.hashCode() : 0);
        result = 31 * result + (readFrom != null ? readFrom.hashCode() : 0);
        result = 31 * result + (initialCursors != null ? initialCursors.hashCode() : 0);
        return result;
    }
}
