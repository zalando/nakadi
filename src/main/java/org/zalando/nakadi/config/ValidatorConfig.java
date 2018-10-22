package org.zalando.nakadi.config;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.io.Resources;
import org.everit.json.schema.Schema;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.zalando.nakadi.domain.SchemaChange;
import org.zalando.nakadi.domain.Version;
import org.zalando.nakadi.service.validation.EventTypeOptionsValidator;
import org.zalando.nakadi.validation.SchemaEvolutionService;
import org.zalando.nakadi.validation.schema.CategoryChangeConstraint;
import org.zalando.nakadi.validation.schema.CompatibilityModeChangeConstraint;
import org.zalando.nakadi.validation.schema.EnrichmentStrategyConstraint;
import org.zalando.nakadi.validation.schema.PartitionKeyFieldsConstraint;
import org.zalando.nakadi.validation.schema.PartitionStrategyConstraint;
import org.zalando.nakadi.validation.schema.SchemaEvolutionConstraint;
import org.zalando.nakadi.validation.schema.diff.SchemaDiff;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.zalando.nakadi.domain.SchemaChange.Type.ADDITIONAL_ITEMS_CHANGED;
import static org.zalando.nakadi.domain.SchemaChange.Type.ADDITIONAL_PROPERTIES_CHANGED;
import static org.zalando.nakadi.domain.SchemaChange.Type.ADDITIONAL_PROPERTIES_NARROWED;
import static org.zalando.nakadi.domain.SchemaChange.Type.ATTRIBUTE_VALUE_CHANGED;
import static org.zalando.nakadi.domain.SchemaChange.Type.COMPOSITION_METHOD_CHANGED;
import static org.zalando.nakadi.domain.SchemaChange.Type.DEPENDENCY_ARRAY_CHANGED;
import static org.zalando.nakadi.domain.SchemaChange.Type.DEPENDENCY_SCHEMA_CHANGED;
import static org.zalando.nakadi.domain.SchemaChange.Type.DEPENDENCY_SCHEMA_REMOVED;
import static org.zalando.nakadi.domain.SchemaChange.Type.DESCRIPTION_CHANGED;
import static org.zalando.nakadi.domain.SchemaChange.Type.ENUM_ARRAY_CHANGED;
import static org.zalando.nakadi.domain.SchemaChange.Type.ID_CHANGED;
import static org.zalando.nakadi.domain.SchemaChange.Type.NUMBER_OF_ITEMS_CHANGED;
import static org.zalando.nakadi.domain.SchemaChange.Type.PROPERTIES_ADDED;
import static org.zalando.nakadi.domain.SchemaChange.Type.PROPERTY_REMOVED;
import static org.zalando.nakadi.domain.SchemaChange.Type.REQUIRED_ARRAY_CHANGED;
import static org.zalando.nakadi.domain.SchemaChange.Type.REQUIRED_ARRAY_EXTENDED;
import static org.zalando.nakadi.domain.SchemaChange.Type.SCHEMA_REMOVED;
import static org.zalando.nakadi.domain.SchemaChange.Type.SUB_SCHEMA_CHANGED;
import static org.zalando.nakadi.domain.SchemaChange.Type.TITLE_CHANGED;
import static org.zalando.nakadi.domain.SchemaChange.Type.TYPE_CHANGED;
import static org.zalando.nakadi.domain.Version.Level.MAJOR;
import static org.zalando.nakadi.domain.Version.Level.MINOR;
import static org.zalando.nakadi.domain.Version.Level.PATCH;

@Configuration
public class ValidatorConfig {

    @Bean
    public EventTypeOptionsValidator eventTypeOptionsValidator(
            @Value("${nakadi.topic.min.retentionMs}") final long minTopicRetentionMs,
            @Value("${nakadi.topic.max.retentionMs}") final long maxTopicRetentionMs) {
        return new EventTypeOptionsValidator(minTopicRetentionMs, maxTopicRetentionMs);
    }

    @Bean
    public SchemaEvolutionService schemaEvolutionService() throws IOException {
        final JSONObject metaSchemaJson = new JSONObject(Resources.toString(Resources.getResource("schema.json"),
                Charsets.UTF_8));
        final Schema metaSchema = SchemaLoader.load(metaSchemaJson);

        final List<SchemaEvolutionConstraint> schemaEvolutionConstraints = Lists.newArrayList(
                new CategoryChangeConstraint(),
                new CompatibilityModeChangeConstraint(),
                new PartitionKeyFieldsConstraint(),
                new PartitionStrategyConstraint(),
                new EnrichmentStrategyConstraint()
        );

        final Map<SchemaChange.Type, Version.Level> compatibleChanges = new HashMap<>();
        compatibleChanges.put(DESCRIPTION_CHANGED, PATCH);
        compatibleChanges.put(TITLE_CHANGED, PATCH);
        compatibleChanges.put(PROPERTIES_ADDED, MINOR);
        compatibleChanges.put(ID_CHANGED, MAJOR);
        compatibleChanges.put(SCHEMA_REMOVED, MAJOR);
        compatibleChanges.put(TYPE_CHANGED, MAJOR);
        compatibleChanges.put(NUMBER_OF_ITEMS_CHANGED, MAJOR);
        compatibleChanges.put(PROPERTY_REMOVED, MAJOR);
        compatibleChanges.put(DEPENDENCY_ARRAY_CHANGED, MAJOR);
        compatibleChanges.put(DEPENDENCY_SCHEMA_CHANGED, MAJOR);
        compatibleChanges.put(COMPOSITION_METHOD_CHANGED, MAJOR);
        compatibleChanges.put(ATTRIBUTE_VALUE_CHANGED, MAJOR);
        compatibleChanges.put(ENUM_ARRAY_CHANGED, MAJOR);
        compatibleChanges.put(SUB_SCHEMA_CHANGED, MAJOR);
        compatibleChanges.put(DEPENDENCY_SCHEMA_REMOVED, MAJOR);
        compatibleChanges.put(REQUIRED_ARRAY_CHANGED, MAJOR);
        compatibleChanges.put(REQUIRED_ARRAY_EXTENDED, MAJOR);
        compatibleChanges.put(ADDITIONAL_PROPERTIES_CHANGED, MAJOR);
        compatibleChanges.put(ADDITIONAL_ITEMS_CHANGED, MAJOR);

        final Map<SchemaChange.Type, Version.Level> forwardChanges = new HashMap<>();
        forwardChanges.put(DESCRIPTION_CHANGED, PATCH);
        forwardChanges.put(TITLE_CHANGED, PATCH);
        forwardChanges.put(PROPERTIES_ADDED, MINOR);
        forwardChanges.put(REQUIRED_ARRAY_EXTENDED, MAJOR);
        forwardChanges.put(ID_CHANGED, MAJOR);
        forwardChanges.put(SCHEMA_REMOVED, MAJOR);
        forwardChanges.put(TYPE_CHANGED, MAJOR);
        forwardChanges.put(NUMBER_OF_ITEMS_CHANGED, MAJOR);
        forwardChanges.put(PROPERTY_REMOVED, MAJOR);
        forwardChanges.put(DEPENDENCY_ARRAY_CHANGED, MAJOR);
        forwardChanges.put(DEPENDENCY_SCHEMA_CHANGED, MAJOR);
        forwardChanges.put(COMPOSITION_METHOD_CHANGED, MAJOR);
        forwardChanges.put(ATTRIBUTE_VALUE_CHANGED, MAJOR);
        forwardChanges.put(ENUM_ARRAY_CHANGED, MAJOR);
        forwardChanges.put(SUB_SCHEMA_CHANGED, MAJOR);
        forwardChanges.put(DEPENDENCY_SCHEMA_REMOVED, MAJOR);
        forwardChanges.put(REQUIRED_ARRAY_CHANGED, MAJOR);
        forwardChanges.put(ADDITIONAL_PROPERTIES_CHANGED, MAJOR);
        forwardChanges.put(ADDITIONAL_PROPERTIES_NARROWED, MINOR);
        forwardChanges.put(ADDITIONAL_ITEMS_CHANGED, MAJOR);

        final Map<SchemaChange.Type, String> errorMessage = new HashMap<>();
        errorMessage.put(SCHEMA_REMOVED, "change not allowed");
        errorMessage.put(TYPE_CHANGED, "schema types must be the same");
        errorMessage.put(NUMBER_OF_ITEMS_CHANGED, "the number of schema items cannot be changed");
        errorMessage.put(PROPERTY_REMOVED, "schema properties cannot be removed");
        errorMessage.put(DEPENDENCY_ARRAY_CHANGED, "schema dependencies array cannot be changed");
        errorMessage.put(DEPENDENCY_SCHEMA_CHANGED, "schema dependencies cannot be changed");
        errorMessage.put(COMPOSITION_METHOD_CHANGED, "schema composition method changed");
        errorMessage.put(ATTRIBUTE_VALUE_CHANGED, "change to attribute value not allowed");
        errorMessage.put(ENUM_ARRAY_CHANGED, "enum array changed");
        errorMessage.put(SUB_SCHEMA_CHANGED, "sub schema changed");
        errorMessage.put(DEPENDENCY_SCHEMA_REMOVED, "dependency schema removed");
        errorMessage.put(REQUIRED_ARRAY_CHANGED, "required array changed");
        errorMessage.put(REQUIRED_ARRAY_EXTENDED, "required array changed");
        errorMessage.put(ADDITIONAL_PROPERTIES_CHANGED, "change not allowed");
        errorMessage.put(ADDITIONAL_PROPERTIES_NARROWED, "change not allowed");
        errorMessage.put(ADDITIONAL_ITEMS_CHANGED, "change not allowed");

        final SchemaDiff diff = new SchemaDiff();

        return new SchemaEvolutionService(metaSchema, schemaEvolutionConstraints, diff, compatibleChanges,
                forwardChanges, errorMessage);
    }
}
