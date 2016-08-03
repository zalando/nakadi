package org.zalando.nakadi.utils;

import com.google.common.collect.Lists;
import org.json.JSONObject;
import org.zalando.nakadi.domain.EnrichmentStrategyDescriptor;
import org.zalando.nakadi.domain.EventCategory;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.EventTypeOptions;
import org.zalando.nakadi.domain.EventTypeSchema;
import org.zalando.nakadi.domain.EventTypeStatistics;
import org.zalando.nakadi.domain.ValidationStrategyConfiguration;
import org.zalando.nakadi.partitioning.PartitionStrategy;

import java.util.List;

public class EventTypeTestBuilder {

    public static final String DEFAULT_OWNING_APPLICATION = "event-producer-application";
    private static final String DEFAULT_SCHEMA = "{ \"price\": 1000 }";

    private String name;
    private String topic;
    private String owningApplication;
    private EventCategory category;
    private final List<ValidationStrategyConfiguration> validationStrategies;
    private List<EnrichmentStrategyDescriptor> enrichmentStrategies;
    private String partitionStrategy;
    private List<String> partitionKeyFields;
    private EventTypeSchema schema;
    private EventTypeStatistics defaultStatistic;
    private EventTypeOptions options;

    public EventTypeTestBuilder() {
        this.name = TestUtils.randomValidEventTypeName();
        this.topic = TestUtils.randomUUID();
        this.owningApplication = DEFAULT_OWNING_APPLICATION;
        this.category = EventCategory.UNDEFINED;
        this.validationStrategies = Lists.newArrayList();
        this.enrichmentStrategies = Lists.newArrayList();
        this.partitionStrategy = PartitionStrategy.RANDOM_STRATEGY;
        this.partitionKeyFields = Lists.newArrayList();
        this.schema = new EventTypeSchema(EventTypeSchema.Type.JSON_SCHEMA, DEFAULT_SCHEMA);
        this.options = new EventTypeOptions();
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
        this.schema = new EventTypeSchema(EventTypeSchema.Type.JSON_SCHEMA, json.toString());
        return this;
    }

    public EventTypeTestBuilder schema(final String json) {
        this.schema = new EventTypeSchema(EventTypeSchema.Type.JSON_SCHEMA, json);
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

    public EventType build() {
        return new EventType(name, topic, owningApplication, category, validationStrategies, enrichmentStrategies,
                partitionStrategy, partitionKeyFields, schema, defaultStatistic, options);
    }

    public static EventTypeTestBuilder builder() {
        return new EventTypeTestBuilder();
    }
}
