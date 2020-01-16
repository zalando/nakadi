package org.zalando.nakadi.utils;

import com.google.common.collect.Lists;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.json.JSONObject;
import org.zalando.nakadi.domain.Audience;
import org.zalando.nakadi.domain.CleanupPolicy;
import org.zalando.nakadi.domain.CompatibilityMode;
import org.zalando.nakadi.domain.EnrichmentStrategyDescriptor;
import org.zalando.nakadi.domain.EventAuthField;
import org.zalando.nakadi.domain.EventCategory;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.EventTypeBase;
import org.zalando.nakadi.domain.EventTypeOptions;
import org.zalando.nakadi.domain.EventTypeSchema;
import org.zalando.nakadi.domain.EventTypeSchemaBase;
import org.zalando.nakadi.domain.EventTypeStatistics;
import org.zalando.nakadi.domain.ResourceAuthorization;
import org.zalando.nakadi.domain.ValidationStrategyConfiguration;
import org.zalando.nakadi.partitioning.PartitionStrategy;

import java.util.List;

import static org.zalando.nakadi.utils.TestUtils.randomDate;

public class EventTypeTestBuilder {

    public static final String DEFAULT_OWNING_APPLICATION = "event-producer-application";
    private static final String DEFAULT_SCHEMA = "{ \"properties\": { \"foo\": { \"type\": \"string\" } } }";
    private final List<ValidationStrategyConfiguration> validationStrategies;
    private String name;
    private String owningApplication;
    private EventCategory category;
    private List<EnrichmentStrategyDescriptor> enrichmentStrategies;
    private String partitionStrategy;
    private List<String> partitionKeyFields;
    private EventTypeSchema schema;
    private EventTypeStatistics defaultStatistic;
    private EventTypeOptions options;
    private CompatibilityMode compatibilityMode;
    private CleanupPolicy cleanupPolicy;
    private DateTime createdAt;
    private DateTime updatedAt;
    private Audience audience;
    private ResourceAuthorization authorization;
    private EventAuthField eventAuthField;


    public EventTypeTestBuilder() {
        this.name = TestUtils.randomValidEventTypeName();
        this.owningApplication = DEFAULT_OWNING_APPLICATION;
        this.category = EventCategory.UNDEFINED;
        this.validationStrategies = Lists.newArrayList();
        this.enrichmentStrategies = Lists.newArrayList();
        this.partitionStrategy = PartitionStrategy.RANDOM_STRATEGY;
        this.partitionKeyFields = Lists.newArrayList();
        this.schema = new EventTypeSchema(new EventTypeSchemaBase(EventTypeSchema.Type.JSON_SCHEMA, DEFAULT_SCHEMA),
                "1.0.0", randomDate());
        this.options = new EventTypeOptions();
        this.options.setRetentionTime(172800000L);
        this.defaultStatistic = new EventTypeStatistics(1,1);
        this.compatibilityMode = CompatibilityMode.COMPATIBLE;
        this.cleanupPolicy = CleanupPolicy.DELETE;
        this.createdAt = new DateTime(DateTimeZone.UTC);
        this.updatedAt = this.createdAt;
        this.authorization = null;
        this.audience = null;
        this.eventAuthField = null;
    }

    public static EventTypeTestBuilder builder() {
        return new EventTypeTestBuilder();
    }

    public EventTypeTestBuilder name(final String name) {
        this.name = name;
        return this;
    }

    public EventTypeTestBuilder owningApplication(final String owningApplication) {
        this.owningApplication = owningApplication;
        return this;
    }

    public EventTypeTestBuilder category(final EventCategory category) {
        this.category = category;
        return this;
    }

    public EventTypeTestBuilder enrichmentStrategies(final List<EnrichmentStrategyDescriptor> enrichmentStrategies) {
        this.enrichmentStrategies = enrichmentStrategies;
        return this;
    }

    public EventTypeTestBuilder partitionStrategy(final String partitionStrategy) {
        this.partitionStrategy = partitionStrategy;
        return this;
    }

    public EventTypeTestBuilder partitionKeyFields(final List<String> partitionKeyFields) {
        this.partitionKeyFields = partitionKeyFields;
        return this;
    }

    public EventTypeTestBuilder schema(final EventTypeSchema schema) {
        this.schema = schema;
        return this;
    }

    public EventTypeTestBuilder schema(final JSONObject json) {
        this.schema = new EventTypeSchema(new EventTypeSchemaBase(EventTypeSchema.Type.JSON_SCHEMA, json.toString()),
                "1.0.0", randomDate());
        return this;
    }

    public EventTypeTestBuilder schema(final String json) {
        this.schema = new EventTypeSchema(new EventTypeSchemaBase(EventTypeSchema.Type.JSON_SCHEMA, json),
                "1.0.0", randomDate());
        return this;
    }

    public EventTypeTestBuilder defaultStatistic(final EventTypeStatistics defaultStatistic) {
        this.defaultStatistic = defaultStatistic;
        return this;
    }

    public EventTypeTestBuilder options(final EventTypeOptions options) {
        this.options = options;
        return this;
    }

    public EventTypeTestBuilder compatibilityMode(final CompatibilityMode compatibilityMode) {
        this.compatibilityMode = compatibilityMode;
        return this;
    }

    public EventTypeTestBuilder cleanupPolicy(final CleanupPolicy cleanupPolicy) {
        this.cleanupPolicy = cleanupPolicy;
        return this;
    }

    public EventTypeTestBuilder createdAt(final DateTime createdAt) {
        this.createdAt = createdAt;
        return this;
    }

    public EventTypeTestBuilder updatedAt(final DateTime updatedAt) {
        this.updatedAt = updatedAt;
        return this;
    }

    public EventTypeTestBuilder authorization(final ResourceAuthorization authorization) {
        this.authorization = authorization;
        return this;
    }

    public EventTypeTestBuilder audience(final Audience audience) {
        this.audience = audience;
        return this;
    }

    public EventTypeTestBuilder eventAuthField(final EventAuthField eventAuthField) {
        this.eventAuthField = eventAuthField;
        return this;
    }

    public EventType build() {
        final EventTypeBase eventTypeBase = new EventTypeBase(name, owningApplication, category,
                validationStrategies, enrichmentStrategies, partitionStrategy, partitionKeyFields, schema,
                defaultStatistic, options, compatibilityMode, cleanupPolicy);
        eventTypeBase.setAuthorization(authorization);
        eventTypeBase.setAudience(audience);
        eventTypeBase.setEventAuthField(eventAuthField);
        return new EventType(eventTypeBase, this.schema.getVersion().toString(), this.createdAt, this.updatedAt);
    }
}
