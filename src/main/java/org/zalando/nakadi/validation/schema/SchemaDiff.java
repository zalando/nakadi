package org.zalando.nakadi.validation.schema;

import org.everit.json.schema.ArraySchema;
import org.everit.json.schema.CombinedSchema;
import org.everit.json.schema.EnumSchema;
import org.everit.json.schema.NumberSchema;
import org.everit.json.schema.ObjectSchema;
import org.everit.json.schema.ReferenceSchema;
import org.everit.json.schema.Schema;
import org.everit.json.schema.StringSchema;
import org.zalando.nakadi.domain.SchemaChange;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.Stack;
import java.util.stream.Collectors;

import static org.everit.json.schema.CombinedSchema.ALL_CRITERION;
import static org.everit.json.schema.CombinedSchema.ANY_CRITERION;
import static org.zalando.nakadi.domain.SchemaChange.Type.ADDITIONAL_ITEMS_CHANGED;
import static org.zalando.nakadi.domain.SchemaChange.Type.ADDITIONAL_PROPERTIES_CHANGED;
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
import static org.zalando.nakadi.domain.SchemaChange.Type.SCHEMA_REMOVED;
import static org.zalando.nakadi.domain.SchemaChange.Type.SUB_SCHEMA_CHANGED;
import static org.zalando.nakadi.domain.SchemaChange.Type.TITLE_CHANGED;
import static org.zalando.nakadi.domain.SchemaChange.Type.TYPE_CHANGED;

public class SchemaDiff {
    public List<SchemaChange> collectChanges(final Schema original, final Schema update) {
        final List<SchemaChange> changes = new ArrayList<>();
        final Stack<String> jsonPath = new Stack<>();

        recursiveCheck(original, update, jsonPath, changes);

        return changes;
    }

    private void recursiveCheck(
            final Schema original,
            final Schema update,
            final Stack<String> jsonPath,
            final List<SchemaChange> changes) {

        if (original == null && update == null) {
            return;
        }

        if (original == null || update == null) {
            addChange(SCHEMA_REMOVED, jsonPath, changes);
            return;
        }

        if (!original.getClass().equals(update.getClass())) {
            addChange(TYPE_CHANGED, jsonPath, changes);
            return;
        }

        if (!Objects.equals(original.getId(), update.getId())) {
            addChange(ID_CHANGED, jsonPath, changes);
        }

        if (!Objects.equals(original.getTitle(), update.getTitle())) {
            addChange(TITLE_CHANGED, jsonPath, changes);
        }

        if (!Objects.equals(original.getDescription(), update.getDescription())) {
            addChange(DESCRIPTION_CHANGED, jsonPath, changes);
        }

        if (original instanceof StringSchema) {
            recursiveCheck((StringSchema) original, (StringSchema) update, jsonPath, changes);
        } else if (original instanceof NumberSchema) {
            recursiveCheck((NumberSchema) original, (NumberSchema) update, jsonPath, changes);
        } else if (original instanceof EnumSchema) {
            recursiveCheck((EnumSchema) original, (EnumSchema) update, jsonPath, changes);
        } else if (original instanceof CombinedSchema) {
            recursiveCheck((CombinedSchema) original, (CombinedSchema) update, jsonPath, changes);
        } else if (original instanceof ObjectSchema) {
            recursiveCheck((ObjectSchema) original, (ObjectSchema) update, jsonPath, changes);
        } else if (original instanceof ArraySchema) {
            recursiveCheck((ArraySchema) original, (ArraySchema) update, jsonPath, changes);
        } else if (original instanceof ReferenceSchema) {
            recursiveCheck((ReferenceSchema) original, (ReferenceSchema) update, jsonPath, changes);
        }
    }

    private void recursiveCheck(final StringSchema stringSchemaOriginal, final StringSchema stringSchemaUpdate,
                                final Stack<String> jsonPath, final List<SchemaChange> changes) {
        if (!Objects.equals(stringSchemaOriginal.getMaxLength(), stringSchemaUpdate.getMaxLength())) {
            jsonPath.push("maxLength");
            addChange(ATTRIBUTE_VALUE_CHANGED, jsonPath, changes);
            jsonPath.pop();
        } else if (!Objects.equals(stringSchemaOriginal.getMinLength(), stringSchemaUpdate.getMinLength())) {
            jsonPath.push("minLength");
            addChange(ATTRIBUTE_VALUE_CHANGED, jsonPath, changes);
            jsonPath.pop();
        } else if (stringSchemaOriginal.getPattern() != stringSchemaUpdate.getPattern()) {
            jsonPath.push("pattern");
            addChange(ATTRIBUTE_VALUE_CHANGED, jsonPath, changes);
            jsonPath.pop();
        }
    }

    private void recursiveCheck(final NumberSchema numberSchemaOriginal, final NumberSchema numberSchemaUpdate,
                                final Stack<String> jsonPath, final List<SchemaChange> changes) {
        if (!Objects.equals(numberSchemaOriginal.getMaximum(), numberSchemaUpdate.getMaximum())) {
            jsonPath.push("maximum");
            addChange(ATTRIBUTE_VALUE_CHANGED, jsonPath, changes);
            jsonPath.pop();
        } else if (!Objects.equals(numberSchemaOriginal.getMinimum(), numberSchemaUpdate.getMinimum())) {
            jsonPath.push("minimum");
            addChange(ATTRIBUTE_VALUE_CHANGED, jsonPath, changes);
            jsonPath.pop();
        } else if (!Objects.equals(numberSchemaOriginal.getMultipleOf(), numberSchemaUpdate.getMultipleOf())) {
            jsonPath.push("multipleOf");
            addChange(ATTRIBUTE_VALUE_CHANGED, jsonPath, changes);
            jsonPath.pop();
        }
    }

    private void recursiveCheck(final EnumSchema enumSchemaOriginal, final EnumSchema enumSchemaUpdate,
                                final Stack<String> jsonPath, final List<SchemaChange> changes) {
        if (!enumSchemaOriginal.getPossibleValues().equals(enumSchemaUpdate.getPossibleValues())) {
            jsonPath.push("enum");
            addChange(ENUM_ARRAY_CHANGED, jsonPath, changes);
            jsonPath.pop();
        }
    }

    private void recursiveCheck(final ReferenceSchema referenceSchemaOriginal,
                                final ReferenceSchema referenceSchemaUpdate, final Stack<String> jsonPath,
                                final List<SchemaChange> changes) {
        jsonPath.push("$ref");
        recursiveCheck(referenceSchemaOriginal.getReferredSchema(), referenceSchemaUpdate.getReferredSchema(),
                jsonPath, changes);
        jsonPath.pop();
    }

    private void recursiveCheck(final ArraySchema original, final ArraySchema update, final Stack<String> jsonPath,
                                final List<SchemaChange> changes) {
        jsonPath.push("items");
        recursiveCheck(original.getAllItemSchema(), update.getAllItemSchema(), jsonPath, changes);
        jsonPath.pop();

        if ((original.getItemSchemas() != null && update.getItemSchemas() == null)
                || (original.getItemSchemas() == null && update.getItemSchemas() != null)) {
            addChange(NUMBER_OF_ITEMS_CHANGED, jsonPath, changes);
        } else if (original.getItemSchemas() != null && update.getItemSchemas() != null) {
            if (original.getItemSchemas().size() != update.getItemSchemas().size()) {
                addChange(NUMBER_OF_ITEMS_CHANGED, jsonPath, changes);
            } else {
                final Iterator<Schema> originalIterator = original.getItemSchemas().iterator();
                final Iterator<Schema> updateIterator = update.getItemSchemas().iterator();
                int index = 0;
                while (originalIterator.hasNext()) {
                    jsonPath.push("items/" + index);
                    recursiveCheck(originalIterator.next(), updateIterator.next(), jsonPath, changes);
                    jsonPath.pop();
                    index += 1;
                }
            }
        }

        jsonPath.push("additionalItems");
        if (original.permitsAdditionalItems() != update.permitsAdditionalItems()) {
            addChange(ADDITIONAL_ITEMS_CHANGED, jsonPath, changes);
        } else {
            recursiveCheck(original.getSchemaOfAdditionalItems(), update.getSchemaOfAdditionalItems(), jsonPath,
                    changes);
        }
        jsonPath.pop();

        if (!Objects.equals(original.getMaxItems(), update.getMaxItems())) {
            jsonPath.push("maxItems");
            addChange(ATTRIBUTE_VALUE_CHANGED, jsonPath, changes);
            jsonPath.pop();
        }

        if (!Objects.equals(original.getMinItems(), update.getMinItems())) {
            jsonPath.push("minItems");
            addChange(ATTRIBUTE_VALUE_CHANGED, jsonPath, changes);
            jsonPath.pop();
        }

        if (original.needsUniqueItems() != update.needsUniqueItems()) {
            jsonPath.push("uniqueItems");
            addChange(ATTRIBUTE_VALUE_CHANGED, jsonPath, changes);
            jsonPath.pop();
        }
    }

    private void recursiveCheck(final ObjectSchema original, final ObjectSchema update,
                                final Stack<String> jsonPath,
                                final List<SchemaChange> changes) {
        jsonPath.push("properties");
        for (final Map.Entry<String, Schema> property : original.getPropertySchemas().entrySet()) {
            jsonPath.push(property.getKey());
            if (!update.getPropertySchemas().containsKey(property.getKey())) {
                addChange(PROPERTY_REMOVED, jsonPath, changes);
            } else {
                recursiveCheck(property.getValue(), update.getPropertySchemas().get(property.getKey()), jsonPath,
                        changes);
            }
            jsonPath.pop();
        }

        if (update.getPropertySchemas().size() > original.getPropertySchemas().size()) {
            addChange(PROPERTIES_ADDED, jsonPath, changes);
        }

        jsonPath.pop();

        jsonPath.push("dependencies");
        for (final Map.Entry<String, Set<String>> dependency : original.getPropertyDependencies().entrySet()) {
            jsonPath.push(dependency.getKey());
            if (!update.getPropertyDependencies().containsKey(dependency.getKey())) {
                addChange(DEPENDENCY_ARRAY_CHANGED, jsonPath, changes);
            } else if (!(dependency.getValue().containsAll(update.getPropertyDependencies().get(dependency.getKey()))
                    && update.getPropertyDependencies().get(dependency.getKey()).containsAll(dependency.getValue()))) {
                addChange(DEPENDENCY_ARRAY_CHANGED, jsonPath, changes);
            }
            jsonPath.pop();
        }

        final List<String> originalDependencies = original.getSchemaDependencies().entrySet().stream()
                .map(Map.Entry::getKey).collect(Collectors.toList());
        final List<String> updateDependencies = update.getSchemaDependencies().entrySet().stream()
                .map(Map.Entry::getKey).collect(Collectors.toList());
        if (!(originalDependencies.containsAll(updateDependencies)
                && updateDependencies.containsAll(originalDependencies))) {
            addChange(DEPENDENCY_SCHEMA_CHANGED, jsonPath, changes);
        } else {
            for (final Map.Entry<String, Schema> dependency : original.getSchemaDependencies().entrySet()) {
                jsonPath.push(dependency.getKey());
                if (!update.getSchemaDependencies().containsKey(dependency.getKey())) {
                    addChange(DEPENDENCY_SCHEMA_REMOVED, jsonPath, changes);
                } else {
                    recursiveCheck(dependency.getValue(), update.getSchemaDependencies().get(dependency.getKey()),
                            jsonPath, changes);
                }
                jsonPath.pop();
            }
        }
        jsonPath.pop();

        jsonPath.push("additionalProperties");
        if (original.permitsAdditionalProperties() != update.permitsAdditionalProperties()) {
            addChange(ADDITIONAL_PROPERTIES_CHANGED, jsonPath, changes);
        } else {
            recursiveCheck(original.getSchemaOfAdditionalProperties(), update.getSchemaOfAdditionalProperties(),
                    jsonPath, changes);
        }
        jsonPath.pop();

        if (!(original.getRequiredProperties().containsAll(update.getRequiredProperties())
                && update.getRequiredProperties().containsAll(original.getRequiredProperties()))) {
            jsonPath.push("required");
            addChange(REQUIRED_ARRAY_CHANGED, jsonPath, changes);
            jsonPath.pop();
        }

        if (!Objects.equals(original.getMaxProperties(), update.getMaxProperties())) {
            jsonPath.push("maxProperties");
            addChange(ATTRIBUTE_VALUE_CHANGED, jsonPath, changes);
            jsonPath.pop();
        }

        if (!Objects.equals(original.getMinProperties(), update.getMinProperties())) {
            jsonPath.push("minProperties");
            addChange(ATTRIBUTE_VALUE_CHANGED, jsonPath, changes);
            jsonPath.pop();
        }
    }

    private void recursiveCheck(final CombinedSchema combinedSchemaOriginal, final CombinedSchema combinedSchemaUpdate,
                                final Stack<String> jsonPath,
                                final List<SchemaChange> changes) {
        if(!(combinedSchemaOriginal.getSubschemas().containsAll(combinedSchemaUpdate.getSubschemas())
                && combinedSchemaUpdate.getSubschemas().containsAll(combinedSchemaOriginal.getSubschemas()))) {
            addChange(SUB_SCHEMA_CHANGED, jsonPath, changes);
        } else {
            if (!combinedSchemaOriginal.getCriterion().equals(combinedSchemaUpdate.getCriterion())) {
                addChange(COMPOSITION_METHOD_CHANGED, jsonPath, changes);
            } else {
                final Iterator<Schema> originalIterator = combinedSchemaOriginal.getSubschemas().iterator();
                final Iterator<Schema> updateIterator = combinedSchemaUpdate.getSubschemas().iterator();
                int index = 0;
                while (originalIterator.hasNext()) {
                    jsonPath.push(validationCriteria(combinedSchemaOriginal.getCriterion()) + "/" + index);
                    recursiveCheck(originalIterator.next(), updateIterator.next(), jsonPath, changes);
                    jsonPath.pop();
                    index += 1;
                }
            }
        }
    }

    private String validationCriteria(final CombinedSchema.ValidationCriterion criterion) {
        if (criterion.equals(ALL_CRITERION)) {
            return "allOf";
        } else if (criterion.equals(ANY_CRITERION)) {
            return "anyOf";
        } else {
            return "oneOf";
        }
    }

    private void addChange(final SchemaChange.Type type, final Stack<String> jsonPath,
                           final List<SchemaChange> changes) {
        changes.add(new SchemaChange(type, jsonPathString(jsonPath)));
    }

    private String jsonPathString(final List<String> jsonPath) {
        return "#/" + String.join("/", jsonPath);
    }
}
