package de.zalando.aruha.nakadi.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.Lists;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.unmodifiableList;

public class EventType {

    public static final List<String> EMPTY_STRING_LIST = new ArrayList<>(0);

    @NotNull
    @Size(min = 1, message = "may not be empty")
    private String name;

    @NotNull
    private String owningApplication;

    @NotNull
    private EventCategory category;

    @JsonIgnore
    private final List<ValidationStrategyConfiguration> validationStrategies = Lists.newArrayList();

    private List<String> orderingKeyFields;

    @NotNull
    @Valid
    private EventTypeSchema schema;

    public String getName() { return name; }

    public void setName(final String name) { this.name = name; }

    public String getOwningApplication() {
        return owningApplication;
    }

    public void setOwningApplication(final String owningApplication) {
        this.owningApplication = owningApplication;
    }

    public EventCategory getCategory() {
        return category;
    }

    public void setCategory(final EventCategory category) {
        this.category = category;
    }

    public List<ValidationStrategyConfiguration> getValidationStrategies() {
        return validationStrategies;
    }

    public EventTypeSchema getSchema() {
        return schema;
    }

    public void setSchema(final EventTypeSchema schema) {
        this.schema = schema;
    }

    public List<String> getOrderingKeyFields() {
        return unmodifiableList(orderingKeyFields != null ? orderingKeyFields : EMPTY_STRING_LIST);
    }

    public void setOrderingKeyFields(final List<String> orderingKeyFields) {
        this.orderingKeyFields = orderingKeyFields;
    }

}
