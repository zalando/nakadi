package org.zalando.nakadi.utils;

import com.google.common.collect.Lists;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.json.JSONObject;
import org.zalando.nakadi.domain.CompatibilityMode;
import org.zalando.nakadi.domain.EnrichmentStrategyDescriptor;
import org.zalando.nakadi.domain.EventCategory;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.EventTypeAuthorization;
import org.zalando.nakadi.domain.EventTypeBase;
import org.zalando.nakadi.domain.EventTypeOptions;
import org.zalando.nakadi.domain.EventTypeSchema;
import org.zalando.nakadi.domain.EventTypeSchemaBase;
import org.zalando.nakadi.domain.EventTypeStatistics;
import org.zalando.nakadi.domain.ValidationStrategyConfiguration;
import org.zalando.nakadi.partitioning.PartitionStrategy;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.zalando.nakadi.utils.TestUtils.randomDate;

public class EventTypeTestBuilder {

    public static final String DEFAULT_OWNING_APPLICATION = "event-producer-application";
    private static final String DEFAULT_SCHEMA = "{ \"properties\": { \"foo\": { \"type\": \"string\" } } }";
    private final List<ValidationStrategyConfiguration> validationStrategies;
    private String name;
    private String topic;
    private String owningApplication;
    private EventCategory category;
    private List<EnrichmentStrategyDescriptor> enrichmentStrategies;
    private String partitionStrategy;
    private List<String> partitionKeyFields;
    private EventTypeSchema schema;
    private EventTypeStatistics defaultStatistic;
    private EventTypeOptions options;
    private Set<String> writeScopes;
    private Set<String> readScopes;
    private CompatibilityMode compatibilityMode;
    private DateTime createdAt;
    private DateTime updatedAt;
    private EventTypeAuthorization authorization;


    public EventTypeTestBuilder() {
        this.name = TestUtils.randomValidEventTypeName();
        this.topic = TestUtils.randomUUID();
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
        this.writeScopes = Collections.emptySet();
        this.readScopes = Collections.emptySet();
        this.compatibilityMode = CompatibilityMode.COMPATIBLE;
        this.createdAt = new DateTime(DateTimeZone.UTC);
        this.updatedAt = this.createdAt;
        this.authorization = null;
    }

    public static EventTypeTestBuilder builder() {
        return new EventTypeTestBuilder();
    }

    public EventTypeTestBuilder name(final String name) {
        this.name = name;
        return this;
    }

    public EventTypeTestBuilder topic(final String topic) {
        this.topic = topic;
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

    public EventTypeTestBuilder writeScopes(final Set<String> writeScopes) {
        this.writeScopes = writeScopes;
        return this;
    }

    public EventTypeTestBuilder readScopes(final Set<String> readScopes) {
        this.readScopes = readScopes;
        return this;
    }

    public EventTypeTestBuilder compatibilityMode(final CompatibilityMode compatibilityMode) {
        this.compatibilityMode = compatibilityMode;
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

    public EventTypeTestBuilder authorization(final EventTypeAuthorization authorization) {
        this.authorization = authorization;
        return this;
    }

    public EventType build() {
        final EventTypeBase eventTypeBase = new EventTypeBase(name, topic, owningApplication, category,
                validationStrategies, enrichmentStrategies, partitionStrategy, partitionKeyFields, schema,
                defaultStatistic, options, writeScopes, readScopes, compatibilityMode);
        eventTypeBase.setAuthorization(authorization);
        return new EventType(eventTypeBase, this.schema.getVersion().toString(), this.createdAt, this.updatedAt);
    }
}
