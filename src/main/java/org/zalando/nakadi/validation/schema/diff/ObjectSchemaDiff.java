package org.zalando.nakadi.validation.schema.diff;

import org.everit.json.schema.ObjectSchema;
import org.everit.json.schema.Schema;
import org.zalando.nakadi.domain.SchemaChange;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.Stack;
import java.util.stream.Collectors;

import static org.zalando.nakadi.domain.SchemaChange.Type.ADDITIONAL_PROPERTIES_CHANGED;
import static org.zalando.nakadi.domain.SchemaChange.Type.ATTRIBUTE_VALUE_CHANGED;
import static org.zalando.nakadi.domain.SchemaChange.Type.DEPENDENCY_ARRAY_CHANGED;
import static org.zalando.nakadi.domain.SchemaChange.Type.DEPENDENCY_SCHEMA_CHANGED;
import static org.zalando.nakadi.domain.SchemaChange.Type.DEPENDENCY_SCHEMA_REMOVED;
import static org.zalando.nakadi.domain.SchemaChange.Type.PROPERTIES_ADDED;
import static org.zalando.nakadi.domain.SchemaChange.Type.PROPERTY_REMOVED;
import static org.zalando.nakadi.domain.SchemaChange.Type.REQUIRED_ARRAY_CHANGED;
import static org.zalando.nakadi.domain.SchemaChange.Type.REQUIRED_ARRAY_EXTENDED;

public class ObjectSchemaDiff {
    static void recursiveCheck(final ObjectSchema original, final ObjectSchema update,
                                       final Stack<String> jsonPath,
                                       final List<SchemaChange> changes) {
        compareProperties(original, update, jsonPath, changes);

        compareDependencies(original, update, jsonPath, changes);

        compareAdditionalProperties(original, update, jsonPath, changes);

        compareAttributes(original, update, jsonPath, changes);
    }

    private static void compareAttributes(final ObjectSchema original, final ObjectSchema update,
                                          final Stack<String> jsonPath, final List<SchemaChange> changes) {

        if (update.getRequiredProperties().containsAll(original.getRequiredProperties())) {
            if (original.getRequiredProperties().size() != update.getRequiredProperties().size()) {
                SchemaDiff.addChange("required", REQUIRED_ARRAY_EXTENDED, jsonPath, changes);
            }
        } else {
            SchemaDiff.addChange("required", REQUIRED_ARRAY_CHANGED, jsonPath, changes);
        }

        if (!Objects.equals(original.getMaxProperties(), update.getMaxProperties())) {
            SchemaDiff.addChange("maxProperties", ATTRIBUTE_VALUE_CHANGED, jsonPath, changes);
        }

        if (!Objects.equals(original.getMinProperties(), update.getMinProperties())) {
            SchemaDiff.addChange("minProperties", ATTRIBUTE_VALUE_CHANGED, jsonPath, changes);
        }
    }

    private static void compareAdditionalProperties(final ObjectSchema original, final ObjectSchema update,
                                                    final Stack<String> jsonPath, final List<SchemaChange> changes) {
        jsonPath.push("additionalProperties");
        if (original.permitsAdditionalProperties() != update.permitsAdditionalProperties()) {
            SchemaDiff.addChange(ADDITIONAL_PROPERTIES_CHANGED, jsonPath, changes);
        } else {
            SchemaDiff.recursiveCheck(original.getSchemaOfAdditionalProperties(),
                    update.getSchemaOfAdditionalProperties(), jsonPath, changes);
        }
        jsonPath.pop();
    }

    private static void compareDependencies(final ObjectSchema original, final ObjectSchema update,
                                            final Stack<String> jsonPath, final List<SchemaChange> changes) {
        jsonPath.push("dependencies");
        for (final Map.Entry<String, Set<String>> dependency : original.getPropertyDependencies().entrySet()) {
            jsonPath.push(dependency.getKey());
            if (!update.getPropertyDependencies().containsKey(dependency.getKey())) {
                SchemaDiff.addChange(DEPENDENCY_ARRAY_CHANGED, jsonPath, changes);
            } else if (!(dependency.getValue().containsAll(update.getPropertyDependencies().get(dependency.getKey()))
                    && update.getPropertyDependencies().get(dependency.getKey()).containsAll(dependency.getValue()))) {
                SchemaDiff.addChange(DEPENDENCY_ARRAY_CHANGED, jsonPath, changes);
            }
            jsonPath.pop();
        }

        final List<String> originalDependencies = original.getSchemaDependencies().entrySet().stream()
                .map(Map.Entry::getKey).collect(Collectors.toList());
        final List<String> updateDependencies = update.getSchemaDependencies().entrySet().stream()
                .map(Map.Entry::getKey).collect(Collectors.toList());
        if (!(originalDependencies.containsAll(updateDependencies)
                && updateDependencies.containsAll(originalDependencies))) {
            SchemaDiff.addChange(DEPENDENCY_SCHEMA_CHANGED, jsonPath, changes);
        } else {
            for (final Map.Entry<String, Schema> dependency : original.getSchemaDependencies().entrySet()) {
                jsonPath.push(dependency.getKey());
                if (!update.getSchemaDependencies().containsKey(dependency.getKey())) {
                    SchemaDiff.addChange(DEPENDENCY_SCHEMA_REMOVED, jsonPath, changes);
                } else {
                    SchemaDiff.recursiveCheck(dependency.getValue(),
                            update.getSchemaDependencies().get(dependency.getKey()), jsonPath, changes);
                }
                jsonPath.pop();
            }
        }
        jsonPath.pop();
    }

    private static void compareProperties(final ObjectSchema original, final ObjectSchema update,
                                          final Stack<String> jsonPath, final List<SchemaChange> changes) {
        jsonPath.push("properties");
        for (final Map.Entry<String, Schema> property : original.getPropertySchemas().entrySet()) {
            jsonPath.push(property.getKey());
            if (!update.getPropertySchemas().containsKey(property.getKey())) {
                SchemaDiff.addChange(PROPERTY_REMOVED, jsonPath, changes);
            } else {
                SchemaDiff.recursiveCheck(property.getValue(), update.getPropertySchemas().get(property.getKey()),
                        jsonPath, changes);
            }
            jsonPath.pop();
        }
        if (update.getPropertySchemas().size() > original.getPropertySchemas().size()) {
            SchemaDiff.addChange(PROPERTIES_ADDED, jsonPath, changes);
        }
        jsonPath.pop();
    }
}
