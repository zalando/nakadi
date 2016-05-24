package de.zalando.aruha.nakadi.domain;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import java.util.Date;
import java.util.List;

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
        return eventTypes;
    }

    public void setEventTypes(final List<String> eventTypes) {
        this.eventTypes = eventTypes;
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
}
