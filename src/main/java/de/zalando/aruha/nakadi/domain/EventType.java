package de.zalando.aruha.nakadi.domain;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.google.common.collect.Lists;

public class EventType {

    private String name;
    private String owningApplication;
    private String type;
    private final List<ValidationStrategyConfiguration> validationStrategies = Lists.newArrayList();
    private EventTypeSchema eventTypeSchema;

    @JsonGetter("name")
    public String getName() {
        return name;
    }

    @JsonSetter("name")
    public void setName(final String name) {
        this.name = name;
    }

    @JsonGetter("owning_application")
    public String getOwningApplication() {
        return owningApplication;
    }

    @JsonSetter("owning_application")
    public void setOwningApplication(String owningApplication) {
        this.owningApplication = owningApplication;
    }

    @JsonGetter("type")
    public String getType() {
        return type;
    }

    @JsonSetter("type")
    public void setType(String type) {
        this.type = type;
    }

    @JsonIgnore
    public List<ValidationStrategyConfiguration> getValidationStrategies() {
        return validationStrategies;
    }

    @JsonGetter("schema")
    public EventTypeSchema getEventTypeSchema() {
        return eventTypeSchema;
    }

    @JsonSetter("schema")
    public void setEventTypeSchema(final EventTypeSchema eventTypeSchema) {
        this.eventTypeSchema = eventTypeSchema;
    }
}
