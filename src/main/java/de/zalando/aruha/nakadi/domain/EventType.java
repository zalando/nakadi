package de.zalando.aruha.nakadi.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.Lists;

import javax.annotation.Nullable;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import java.util.ArrayList;
import java.util.List;

import static de.zalando.aruha.nakadi.service.StrategiesRegistry.RANDOM_PARTITION_STRATEGY;
import static java.util.Collections.unmodifiableList;

public class EventType {

    public static final List<String> EMPTY_STRING_LIST = new ArrayList<>(0);

    @NotNull
    @Pattern(regexp = "[a-zA-Z][-0-9a-zA-Z_]*(\\.[a-zA-Z][-0-9a-zA-Z_]*)*", message = "format not allowed" )
    private String name;

    @NotNull
    private String owningApplication;

    @NotNull
    private EventCategory category;

    @JsonIgnore
    private final List<ValidationStrategyConfiguration> validationStrategies = Lists.newArrayList();

    @Valid
    private PartitionStrategyDescriptor partitionStrategy = RANDOM_PARTITION_STRATEGY;

    @Nullable
    private List<String> partitionKeyFields;

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

    public PartitionStrategyDescriptor getPartitionStrategy() {
        return partitionStrategy;
    }

    public void setPartitionStrategy(final PartitionStrategyDescriptor partitionStrategyDescriptor) {
        this.partitionStrategy = partitionStrategyDescriptor;
    }

    public EventTypeSchema getSchema() {
        return schema;
    }

    public void setSchema(final EventTypeSchema schema) {
        this.schema = schema;
    }

    public List<String> getPartitionKeyFields() {
        return unmodifiableList(partitionKeyFields != null ? partitionKeyFields : EMPTY_STRING_LIST);
    }

    public void setPartitionKeyFields(final List<String> partitionKeyFields) {
        this.partitionKeyFields = partitionKeyFields;
    }

}
