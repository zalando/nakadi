package org.zalando.nakadi.config;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.io.Resources;
import org.everit.json.schema.Schema;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONObject;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.zalando.nakadi.domain.SchemaChange;
import org.zalando.nakadi.service.AvroSchemaCompatibility;
import org.zalando.nakadi.service.SchemaEvolutionService;
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
import static org.zalando.nakadi.domain.SchemaChange.Type.ENUM_ARRAY_CHANGED;
import static org.zalando.nakadi.domain.SchemaChange.Type.NUMBER_OF_ITEMS_CHANGED;
import static org.zalando.nakadi.domain.SchemaChange.Type.PROPERTY_REMOVED;
import static org.zalando.nakadi.domain.SchemaChange.Type.REQUIRED_ARRAY_CHANGED;
import static org.zalando.nakadi.domain.SchemaChange.Type.REQUIRED_ARRAY_EXTENDED;
import static org.zalando.nakadi.domain.SchemaChange.Type.SCHEMA_REMOVED;
import static org.zalando.nakadi.domain.SchemaChange.Type.SUB_SCHEMA_CHANGED;
import static org.zalando.nakadi.domain.SchemaChange.Type.TYPE_CHANGED;
import static org.zalando.nakadi.domain.SchemaChange.Type.TYPE_NARROWED;

@Configuration
public class SchemaValidatorConfig {

    private final CompatibilityModeChangeConstraint compatibilityModeChangeConstraint;
    private final PartitionStrategyConstraint partitionStrategyConstraint;
    private final AvroSchemaCompatibility avroSchemaCompatibility;

    public SchemaValidatorConfig(
            final CompatibilityModeChangeConstraint compatibilityModeChangeConstraint,
            final PartitionStrategyConstraint partitionStrategyConstraint,
            final AvroSchemaCompatibility avroSchemaCompatibility) {
        this.compatibilityModeChangeConstraint = compatibilityModeChangeConstraint;
        this.partitionStrategyConstraint = partitionStrategyConstraint;
        this.avroSchemaCompatibility = avroSchemaCompatibility;
    }

    @Bean
    public SchemaEvolutionService schemaEvolutionService() throws IOException {
        final JSONObject metaSchemaJson = new JSONObject(Resources.toString(Resources.getResource("schema.json"),
                Charsets.UTF_8));
        final Schema metaSchema = SchemaLoader.load(metaSchemaJson);

        final List<SchemaEvolutionConstraint> schemaEvolutionConstraints = Lists.newArrayList(
                new CategoryChangeConstraint(),
                compatibilityModeChangeConstraint,
                new PartitionKeyFieldsConstraint(),
                partitionStrategyConstraint,
                new EnrichmentStrategyConstraint()
        );

        final Map<SchemaChange.Type, String> errorMessage = new HashMap<>();
        errorMessage.put(SCHEMA_REMOVED, "change not allowed");
        errorMessage.put(TYPE_NARROWED, "schema types cannot be narrowed");
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

        return new SchemaEvolutionService(metaSchema, schemaEvolutionConstraints, diff, errorMessage,
                avroSchemaCompatibility);
    }
}
