package org.zalando.nakadi.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.Lists;
import org.zalando.nakadi.partitioning.PartitionStrategy;

import javax.annotation.Nullable;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import javax.validation.constraints.Size;
import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.unmodifiableList;

public class EventType {

    public static final List<String> EMPTY_STRING_LIST = new ArrayList<>(0);

    @NotNull
    @Pattern(regexp = "[a-zA-Z][-0-9a-zA-Z_]*(\\.[a-zA-Z][-0-9a-zA-Z_]*)*", message = "format not allowed" )
    @Size(min = 1, max = 255, message = "the length of the name must be >= 1 and <= 255")
    private String name;

    @JsonIgnore
    private String topic;

    @NotNull
    private String owningApplication;

    @NotNull
    private EventCategory category;

    @JsonIgnore
    private List<ValidationStrategyConfiguration> validationStrategies = Lists.newArrayList();

    @NotNull
    private List<EnrichmentStrategyDescriptor> enrichmentStrategies = Lists.newArrayList();

    private String partitionStrategy = PartitionStrategy.RANDOM_STRATEGY;

    @Nullable
    private List<String> partitionKeyFields;

    @Valid
    @NotNull
    private EventTypeSchema schema;

    @Valid
    @Nullable
    private EventTypeStatistics defaultStatistic;

    @Valid
    @Nullable
    private EventTypeOptions options;

    public EventType() {}

    public EventType(final String name, final String topic, final String owningApplication, final EventCategory category,
                     final List<ValidationStrategyConfiguration> validationStrategies,
                     final List<EnrichmentStrategyDescriptor> enrichmentStrategies, final String partitionStrategy,
                     final List<String> partitionKeyFields, final EventTypeSchema schema, final EventTypeStatistics defaultStatistic,
                     final EventTypeOptions options)
    {
        this.name = name;
        this.topic = topic;
        this.owningApplication = owningApplication;
        this.category = category;
        this.validationStrategies = validationStrategies;
        this.enrichmentStrategies = enrichmentStrategies;
        this.partitionStrategy = partitionStrategy;
        this.partitionKeyFields = partitionKeyFields;
        this.schema = schema;
        this.defaultStatistic = defaultStatistic;
        this.options = options;
    }

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

    public String getPartitionStrategy() {
        return partitionStrategy;
    }

    public void setPartitionStrategy(final String partitionStrategy) {
        this.partitionStrategy = partitionStrategy;
    }

    public EventTypeSchema getSchema() {
        return schema;
    }

    public void setSchema(final EventTypeSchema schema) {
        this.schema = schema;
    }

    public EventTypeStatistics getDefaultStatistic() {
        return defaultStatistic;
    }

    public void setDefaultStatistic(final EventTypeStatistics defaultStatistic) {
        this.defaultStatistic = defaultStatistic;
    }

    public List<String> getPartitionKeyFields() {
        return unmodifiableList(partitionKeyFields != null ? partitionKeyFields : EMPTY_STRING_LIST);
    }

    public void setPartitionKeyFields(final List<String> partitionKeyFields) {
        this.partitionKeyFields = partitionKeyFields;
    }

    public List<EnrichmentStrategyDescriptor> getEnrichmentStrategies() {
        return enrichmentStrategies;
    }

    public void setEnrichmentStrategies(final List<EnrichmentStrategyDescriptor> enrichmentStrategies) {
        this.enrichmentStrategies = enrichmentStrategies;
    }

    public String getTopic() { return topic; }

    public void setTopic(final String topic) { this.topic = topic; }

    public EventTypeOptions getOptions() {
        return options;
    }

    public void setOptions(final EventTypeOptions options) {
        this.options = options;
    }
}
