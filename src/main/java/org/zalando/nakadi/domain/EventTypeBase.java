package org.zalando.nakadi.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.zalando.nakadi.partitioning.PartitionStrategy;

import javax.annotation.Nullable;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import javax.validation.constraints.Size;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static java.util.Collections.unmodifiableList;

public class EventTypeBase {
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
    private List<ValidationStrategyConfiguration> validationStrategies;

    @NotNull
    private List<EnrichmentStrategyDescriptor> enrichmentStrategies;

    private String partitionStrategy;

    @Nullable
    private List<String> partitionKeyFields;

    @Valid
    @NotNull
    private EventTypeSchemaBase schema;

    @Valid
    @Nullable
    private EventTypeStatistics defaultStatistic;

    @Valid
    private EventTypeOptions options;

    @Valid
    private EventTypeAuthorization authorization;

    private Set<String> writeScopes;

    private Set<String> readScopes;

    private CompatibilityMode compatibilityMode;

    public EventTypeBase() {
        this.validationStrategies = Collections.emptyList();
        this.enrichmentStrategies = Collections.emptyList();
        this.partitionStrategy = PartitionStrategy.RANDOM_STRATEGY;
        this.options = new EventTypeOptions();
        this.writeScopes = Collections.emptySet();
        this.readScopes = Collections.emptySet();
        this.compatibilityMode = CompatibilityMode.FORWARD;
    }

    public EventTypeBase(final String name, final String topic, final String owningApplication,
                     final EventCategory category,
                     final List<ValidationStrategyConfiguration> validationStrategies,
                     final List<EnrichmentStrategyDescriptor> enrichmentStrategies,
                     final String partitionStrategy,
                     final List<String> partitionKeyFields, final EventTypeSchemaBase schema,
                     final EventTypeStatistics defaultStatistic,
                     final EventTypeOptions options, final Set<String> writeScopes,
                     final Set<String> readScopes,
                     final CompatibilityMode compatibilityMode) {
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
        this.writeScopes = writeScopes;
        this.readScopes = readScopes;
        this.compatibilityMode = compatibilityMode;
    }

    public EventTypeBase(final EventTypeBase eventType) {
        this.setName(eventType.getName());
        this.setTopic(eventType.getTopic());
        this.setOwningApplication(eventType.getOwningApplication());
        this.setCategory(eventType.getCategory());
        this.setValidationStrategies(eventType.getValidationStrategies());
        this.setEnrichmentStrategies(eventType.getEnrichmentStrategies());
        this.setPartitionStrategy(eventType.getPartitionStrategy());
        this.setPartitionKeyFields(eventType.getPartitionKeyFields());
        this.setSchema(eventType.getSchema());
        this.setDefaultStatistic(eventType.getDefaultStatistic());
        this.setOptions(eventType.getOptions());
        this.setWriteScopes(eventType.getWriteScopes());
        this.setReadScopes(eventType.getReadScopes());
        this.setCompatibilityMode(eventType.getCompatibilityMode());
        this.setAuthorization(eventType.getAuthorization());
    }

    public String getName() {
        return name;
    }

    public void setName(final String name) {
        this.name = name;
    }

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

    public EventTypeSchemaBase getSchema() {
        return schema;
    }

    public void setSchema(final EventTypeSchemaBase schema) {
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

    public String getTopic() {
        return topic;
    }

    public void setTopic(final String topic) {
        this.topic = topic;
    }

    public EventTypeOptions getOptions() {
        return options;
    }

    public void setOptions(final EventTypeOptions options) {
        this.options = options;
    }

    public Set<String> getWriteScopes() {
        return Collections.unmodifiableSet(writeScopes);
    }

    public void setWriteScopes(final Set<String> writeScopes) {
        this.writeScopes = writeScopes == null ? Collections.emptySet() : writeScopes;
    }

    public Set<String> getReadScopes() {
        return Collections.unmodifiableSet(readScopes);
    }

    public void setReadScopes(final Set<String> readScopes) {
        this.readScopes = readScopes == null ? Collections.emptySet() : readScopes;
    }

    public CompatibilityMode getCompatibilityMode() {
        return compatibilityMode;
    }

    public void setCompatibilityMode(final CompatibilityMode compatibilityMode) {
        this.compatibilityMode = compatibilityMode;
    }

    public void setValidationStrategies(final List<ValidationStrategyConfiguration> validationStrategies) {
        this.validationStrategies = validationStrategies;
    }

    @Nullable
    public EventTypeAuthorization getAuthorization() {
        return authorization;
    }

    public void setAuthorization(final EventTypeAuthorization authorization) {
        this.authorization = authorization;
    }
}
