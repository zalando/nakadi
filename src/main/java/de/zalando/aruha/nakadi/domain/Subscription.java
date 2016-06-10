package de.zalando.aruha.nakadi.domain;

import org.joda.time.DateTime;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import javax.validation.constraints.Size;
import java.util.Collections;
import java.util.Set;

import static com.google.common.collect.Sets.newTreeSet;

public class Subscription {

    public static String POSITION_BEGIN = "BEGIN";
    public static String POSITION_END = "END";

    @Nullable
    private String id;

    @NotNull
    private String owningApplication;

    @NotNull
    @Size(min = 1, max = 1)
    private Set<String> eventTypes;

    @NotNull
    private String consumerGroup = "none";

    @Nullable
    private DateTime createdAt;

    @NotNull
    @Pattern(regexp = "^(BEGIN|END)$", message = "value not allowed, possible values are: 'BEGIN', 'END'" )
    private String startFrom = POSITION_END;

    public String getId() {
        return id;
    }

    public void setId(final String id) {
        this.id = id;
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

    public DateTime getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(final DateTime createdAt) {
        this.createdAt = createdAt;
    }

    public String getStartFrom() {
        return startFrom;
    }

    public void setStartFrom(final String startFrom) {
        this.startFrom = startFrom;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final Subscription that = (Subscription) o;
        return id != null ? id.equals(that.id) : that.id == null
                && owningApplication.equals(that.owningApplication)
                && eventTypes.equals(that.eventTypes)
                && consumerGroup.equals(that.consumerGroup)
                && (createdAt != null ? createdAt.equals(that.createdAt) : that.createdAt == null
                && startFrom.equals(that.startFrom));
    }

    @Override
    public int hashCode() {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + (owningApplication != null ? owningApplication.hashCode() : 0);
        result = 31 * result + (eventTypes != null ? eventTypes.hashCode() : 0);
        result = 31 * result + (consumerGroup != null ? consumerGroup.hashCode() : 0);
        result = 31 * result + (createdAt != null ? createdAt.hashCode() : 0);
        result = 31 * result + (startFrom != null ? startFrom.hashCode() : 0);
        return result;
    }
}
