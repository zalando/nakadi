package org.zalando.nakadi.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.google.common.collect.ImmutableList;
import org.zalando.nakadi.annotations.validation.AnnotationKey;
import org.zalando.nakadi.annotations.validation.AnnotationValue;
import org.zalando.nakadi.annotations.validation.LabelKey;
import org.zalando.nakadi.annotations.validation.LabelValue;
import org.zalando.nakadi.partitioning.PartitionStrategy;
import org.zalando.nakadi.plugin.api.authz.EventTypeAuthz;
import org.zalando.nakadi.plugin.api.authz.Resource;
import org.zalando.nakadi.view.EventOwnerSelector;

import javax.annotation.Nullable;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import javax.validation.constraints.Size;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.util.Collections.unmodifiableList;

public class EventTypeBase implements EventTypeAuthz {

    public static final String DATA_CHANGE_WRAP_FIELD = "data";
    public static final String DATA_PATH_PREFIX = DATA_CHANGE_WRAP_FIELD + ".";
    private static final List<String> EMPTY_PARTITION_KEY_FIELDS = ImmutableList.of();
    private static final List<String> EMPTY_ORDERING_KEY_FIELDS = ImmutableList.of();

    @NotNull
    @Pattern(regexp = "[a-zA-Z][-0-9a-zA-Z_]*(\\.[0-9a-zA-Z][-0-9a-zA-Z_]*)*", message = "format not allowed")
    @Size(min = 1, max = 255, message = "the length of the name must be >= 1 and <= 255")
    private String name;

    @NotNull
    private String owningApplication;

    @NotNull
    private EventCategory category;

    @NotNull
    private List<EnrichmentStrategyDescriptor> enrichmentStrategies;

    private String partitionStrategy;

    @Nullable
    private List<String> partitionKeyFields;

    @NotNull
    private CleanupPolicy cleanupPolicy;

    @Nullable
    private List<String> orderingKeyFields;

    @Nullable
    private List<String> orderingInstanceIds;

    @Valid
    @NotNull
    private EventTypeSchemaBase schema;

    @Valid
    @Nullable
    private EventTypeStatistics defaultStatistic;

    @Valid
    private EventTypeOptions options;

    @Valid
    private ResourceAuthorization authorization;

    @Valid
    @Nullable
    private Map<
            @AnnotationKey String,
            @AnnotationValue String> annotations;

    @Valid
    @Nullable
    private Map<
            @LabelKey String,
            @LabelValue String> labels;

    @NotNull
    private CompatibilityMode compatibilityMode;

    @Nullable
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private Audience audience;

    @Valid
    @Nullable
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private EventOwnerSelector eventOwnerSelector;

    public EventTypeBase() {
        this.enrichmentStrategies = Collections.emptyList();
        this.partitionStrategy = PartitionStrategy.RANDOM_STRATEGY;
        this.options = new EventTypeOptions();
        this.compatibilityMode = CompatibilityMode.FORWARD;
        this.cleanupPolicy = CleanupPolicy.DELETE;
    }

    public EventTypeBase(final String name,
                         final String owningApplication,
                         final EventCategory category,
                         final List<EnrichmentStrategyDescriptor> enrichmentStrategies,
                         final String partitionStrategy,
                         final List<String> partitionKeyFields,
                         final EventTypeSchemaBase schema,
                         final EventTypeStatistics defaultStatistic,
                         final EventTypeOptions options,
                         final CompatibilityMode compatibilityMode,
                         final CleanupPolicy cleanupPolicy,
                         final Map<String, String> annotations,
                         final Map<String, String> labels) {
        this.name = name;
        this.owningApplication = owningApplication;
        this.category = category;
        this.enrichmentStrategies = enrichmentStrategies;
        this.partitionStrategy = partitionStrategy;
        this.partitionKeyFields = partitionKeyFields;
        this.schema = schema;
        this.defaultStatistic = defaultStatistic;
        this.options = options;
        this.compatibilityMode = compatibilityMode;
        this.cleanupPolicy = cleanupPolicy;
        this.annotations = annotations;
        this.labels = labels;
    }

    public EventTypeBase(final EventTypeBase eventType) {
        this.setName(eventType.getName());
        this.setOwningApplication(eventType.getOwningApplication());
        this.setCategory(eventType.getCategory());
        this.setEnrichmentStrategies(eventType.getEnrichmentStrategies());
        this.setPartitionStrategy(eventType.getPartitionStrategy());
        this.setPartitionKeyFields(eventType.getPartitionKeyFields());
        this.setSchema(eventType.getSchema());
        this.setDefaultStatistic(eventType.getDefaultStatistic());
        this.setOptions(eventType.getOptions());
        this.setCompatibilityMode(eventType.getCompatibilityMode());
        this.setAuthorization(eventType.getAuthorization());
        this.setAudience(eventType.getAudience());
        this.setOrderingKeyFields(eventType.getOrderingKeyFields());
        this.setOrderingInstanceIds(eventType.getOrderingInstanceIds());
        this.setCleanupPolicy(eventType.getCleanupPolicy());
        this.setEventOwnerSelector(eventType.getEventOwnerSelector());
        this.setAnnotations(eventType.getAnnotations());
        this.setLabels(eventType.getLabels());
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
        return unmodifiableList(partitionKeyFields != null ? partitionKeyFields : EMPTY_PARTITION_KEY_FIELDS);
    }

    public void setPartitionKeyFields(final List<String> partitionKeyFields) {
        this.partitionKeyFields = partitionKeyFields;
    }

    public CleanupPolicy getCleanupPolicy() {
        return cleanupPolicy;
    }

    public void setCleanupPolicy(final CleanupPolicy cleanupPolicy) {
        this.cleanupPolicy = cleanupPolicy;
    }

    public List<String> getOrderingKeyFields() {
        return unmodifiableList(orderingKeyFields != null ? orderingKeyFields : EMPTY_ORDERING_KEY_FIELDS);
    }

    public void setOrderingKeyFields(@Nullable final List<String> orderingKeyFields) {
        this.orderingKeyFields = orderingKeyFields;
    }

    public List<String> getOrderingInstanceIds() {
        return unmodifiableList(orderingInstanceIds != null ? orderingInstanceIds : EMPTY_ORDERING_KEY_FIELDS);
    }

    public void setOrderingInstanceIds(@Nullable final List<String> orderingInstanceIds) {
        this.orderingInstanceIds = orderingInstanceIds;
    }

    public List<EnrichmentStrategyDescriptor> getEnrichmentStrategies() {
        return enrichmentStrategies;
    }

    public void setEnrichmentStrategies(final List<EnrichmentStrategyDescriptor> enrichmentStrategies) {
        this.enrichmentStrategies = enrichmentStrategies;
    }

    public EventTypeOptions getOptions() {
        return options;
    }

    public void setOptions(final EventTypeOptions options) {
        this.options = options;
    }

    public CompatibilityMode getCompatibilityMode() {
        return compatibilityMode;
    }

    public void setCompatibilityMode(final CompatibilityMode compatibilityMode) {
        this.compatibilityMode = compatibilityMode;
    }

    @Nullable
    public EventOwnerSelector getEventOwnerSelector() {
        return eventOwnerSelector;
    }

    public void setEventOwnerSelector(@Nullable final EventOwnerSelector eventOwnerSelector) {
        this.eventOwnerSelector = eventOwnerSelector;
    }

    @Nullable
    public ResourceAuthorization getAuthorization() {
        return authorization;
    }

    @Nullable
    public Audience getAudience() {
        return audience;
    }

    public void setAudience(@Nullable final Audience audience) {
        this.audience = audience;
    }

    public void setAuthorization(final ResourceAuthorization authorization) {
        this.authorization = authorization;
    }

    @JsonIgnore
    @Override
    public String getAuthCompatibilityMode() {
        return this.compatibilityMode.toString();
    }

    @JsonIgnore
    @Override
    public String getAuthCleanupPolicy() {
        return this.cleanupPolicy.toString();
    }

    @JsonIgnore
    public Resource<EventTypeBase> asBaseResource() {
        return new ResourceImpl<>(getName(), ResourceImpl.EVENT_TYPE_RESOURCE, getAuthorization(), this);
    }

    public void setAnnotations(final Map<String, String> annotations) {
        this.annotations = annotations;
    }

    public Map<String, String> getAnnotations() {
        return this.annotations;
    }

    public void setLabels(final Map<String, String> labels) {
        this.labels = labels;
    }

    public Map<String, String> getLabels() {
        return this.labels;
    }
}
