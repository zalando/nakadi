package de.zalando.aruha.nakadi.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import java.util.List;

public class EventType {

    @NotNull
    @Size(min = 1, message = "may not be empty")
    private String name;

    private String owningApplication;

    @NotNull
    @Size(min = 1, message = "may not be empty")
    private String category;

    @JsonIgnore
    private final List<ValidationStrategyConfiguration> validationStrategies = Lists.newArrayList();

    @JsonProperty("schema")
    @NotNull
    @Valid
    private EventTypeSchema eventTypeSchema;

    public String getName() { return name; }

    public void setName(final String name) { this.name = name; }

    public String getOwningApplication() {
        return owningApplication;
    }

    public void setOwningApplication(String owningApplication) {
        this.owningApplication = owningApplication;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public List<ValidationStrategyConfiguration> getValidationStrategies() {
        return validationStrategies;
    }

    public EventTypeSchema getEventTypeSchema() {
        return eventTypeSchema;
    }

    public void setEventTypeSchema(final EventTypeSchema eventTypeSchema) {
        this.eventTypeSchema = eventTypeSchema;
    }
}
