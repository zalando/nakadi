package de.zalando.aruha.nakadi.domain;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import static com.google.common.collect.Lists.newArrayList;

public class Subscription {

    @Nullable
    private String id;

    @NotNull
    private String owningApplication;

    @NotNull
    @Size(min = 1, max = 1)
    private List<String> eventTypes;

    private String useCase = "none";

    private Date createdAt = new Date();

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

    public List<String> getEventTypes() {
        return Collections.unmodifiableList(eventTypes);
    }

    public void setEventTypes(final List<String> eventTypes) {
        this.eventTypes = newArrayList(eventTypes);
        Collections.sort(this.eventTypes);
    }

    public String getUseCase() {
        return useCase;
    }

    public void setUseCase(final String useCase) {
        this.useCase = useCase;
    }

    public Date getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(final Date createdAt) {
        this.createdAt = createdAt;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final Subscription that = (Subscription) o;
        return id != null ? id.equals(that.id) : that.id == null && owningApplication.equals(that.owningApplication) &&
                eventTypes.equals(that.eventTypes) && useCase.equals(that.useCase) && createdAt.equals(that.createdAt);
    }

    @Override
    public int hashCode() {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + owningApplication.hashCode();
        result = 31 * result + eventTypes.hashCode();
        result = 31 * result + useCase.hashCode();
        result = 31 * result + createdAt.hashCode();
        return result;
    }
}
