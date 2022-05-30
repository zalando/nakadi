package org.zalando.nakadi.validation.schema.diff;

import org.everit.json.schema.ObjectSchema;
import org.everit.json.schema.Schema;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.zalando.nakadi.domain.SchemaChange.Type.ADDITIONAL_PROPERTIES_CHANGED;
import static org.zalando.nakadi.domain.SchemaChange.Type.ADDITIONAL_PROPERTIES_NARROWED;
import static org.zalando.nakadi.domain.SchemaChange.Type.ATTRIBUTE_VALUE_CHANGED;
import static org.zalando.nakadi.domain.SchemaChange.Type.DEPENDENCY_ARRAY_CHANGED;
import static org.zalando.nakadi.domain.SchemaChange.Type.DEPENDENCY_SCHEMA_CHANGED;
import static org.zalando.nakadi.domain.SchemaChange.Type.DEPENDENCY_SCHEMA_REMOVED;
import static org.zalando.nakadi.domain.SchemaChange.Type.PROPERTIES_ADDED;
import static org.zalando.nakadi.domain.SchemaChange.Type.PROPERTY_REMOVED;
import static org.zalando.nakadi.domain.SchemaChange.Type.REQUIRED_ARRAY_CHANGED;
import static org.zalando.nakadi.domain.SchemaChange.Type.REQUIRED_ARRAY_EXTENDED;

public class ObjectSchemaDiff {
    static void recursiveCheck(final ObjectSchema original, final ObjectSchema update, final SchemaDiffState state) {
        compareProperties(original, update, state);

        compareDependencies(original, update, state);

        compareAdditionalProperties(original, update, state);

        compareAttributes(original, update, state);
    }

    private static void compareAttributes(
            final ObjectSchema original, final ObjectSchema update, final SchemaDiffState state) {

        if (update.getRequiredProperties().containsAll(original.getRequiredProperties())) {
            if (original.getRequiredProperties().size() != update.getRequiredProperties().size()) {
                state.addChange("required", REQUIRED_ARRAY_EXTENDED);
            }
        } else {
            state.addChange("required", REQUIRED_ARRAY_CHANGED);
        }

        if (!Objects.equals(original.getMaxProperties(), update.getMaxProperties())) {
            state.addChange("maxProperties", ATTRIBUTE_VALUE_CHANGED);
        }

        if (!Objects.equals(original.getMinProperties(), update.getMinProperties())) {
            state.addChange("minProperties", ATTRIBUTE_VALUE_CHANGED);
        }
    }

    private static void compareAdditionalProperties(
            final ObjectSchema original, final ObjectSchema update, final SchemaDiffState state) {
        state.runOnPath("additionalProperties", () -> {
            if (original.permitsAdditionalProperties() != update.permitsAdditionalProperties()) {
                state.addChange(ADDITIONAL_PROPERTIES_CHANGED);
            } else if (original.getSchemaOfAdditionalProperties() == null &&
                    update.getSchemaOfAdditionalProperties() != null) {
                state.addChange(ADDITIONAL_PROPERTIES_NARROWED);
            } else {
                SchemaDiff.recursiveCheck(
                        original.getSchemaOfAdditionalProperties(), update.getSchemaOfAdditionalProperties(), state);
            }
        });
    }

    private static void compareDependencies(
            final ObjectSchema original, final ObjectSchema update, final SchemaDiffState state) {
        state.runOnPath("dependencies", () -> {
            for (final Map.Entry<String, Set<String>> dependency : original.getPropertyDependencies().entrySet()) {
                state.runOnPath(dependency.getKey(), () -> {
                    if (!update.getPropertyDependencies().containsKey(dependency.getKey())) {
                        state.addChange(DEPENDENCY_ARRAY_CHANGED);
                    } else {
                        final boolean hasAllInFirst = dependency.getValue().containsAll(
                                update.getPropertyDependencies().get(dependency.getKey()));
                        final boolean hasAllInSecond = update.getPropertyDependencies().get(dependency.getKey())
                                .containsAll(dependency.getValue());
                        if (!hasAllInFirst || !hasAllInSecond) {
                            state.addChange(DEPENDENCY_ARRAY_CHANGED);
                        }
                    }
                });
            }

            final List<String> originalDependencies = original.getSchemaDependencies().entrySet().stream()
                    .map(Map.Entry::getKey).collect(Collectors.toList());
            final List<String> updateDependencies = update.getSchemaDependencies().entrySet().stream()
                    .map(Map.Entry::getKey).collect(Collectors.toList());
            if (!(originalDependencies.containsAll(updateDependencies)
                    && updateDependencies.containsAll(originalDependencies))) {
                state.addChange(DEPENDENCY_SCHEMA_CHANGED);
            } else {
                for (final Map.Entry<String, Schema> dependency : original.getSchemaDependencies().entrySet()) {
                    state.runOnPath(dependency.getKey(), () -> {
                        if (!update.getSchemaDependencies().containsKey(dependency.getKey())) {
                            state.addChange(DEPENDENCY_SCHEMA_REMOVED);
                        } else {
                            SchemaDiff.recursiveCheck(
                                    dependency.getValue(),
                                    update.getSchemaDependencies().get(dependency.getKey()),
                                    state);
                        }
                    });
                }
            }
        });
    }

    private static void compareProperties(
            final ObjectSchema original, final ObjectSchema update, final SchemaDiffState state) {
        state.runOnPath("properties", () -> {
            for (final Map.Entry<String, Schema> property : original.getPropertySchemas().entrySet()) {
                state.runOnPath(property.getKey(), () -> {
                    if (!update.getPropertySchemas().containsKey(property.getKey())) {
                        state.addChange(PROPERTY_REMOVED);
                    } else {
                        SchemaDiff.recursiveCheck(
                                property.getValue(), update.getPropertySchemas().get(property.getKey()), state);
                    }
                });
            }
            if (update.getPropertySchemas().size() > original.getPropertySchemas().size()) {
                state.addChange(PROPERTIES_ADDED);
            }
        });
    }
}
