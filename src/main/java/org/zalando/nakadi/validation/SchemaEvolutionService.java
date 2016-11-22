package org.zalando.nakadi.validation;


import org.everit.json.schema.ArraySchema;
import org.everit.json.schema.CombinedSchema;
import org.everit.json.schema.EnumSchema;
import org.everit.json.schema.NumberSchema;
import org.everit.json.schema.ObjectSchema;
import org.everit.json.schema.ReferenceSchema;
import org.everit.json.schema.Schema;
import org.everit.json.schema.StringSchema;
import org.everit.json.schema.ValidationException;
import org.everit.json.schema.loader.SchemaLoader;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.json.JSONObject;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.EventTypeBase;
import org.zalando.nakadi.exceptions.InvalidEventTypeException;
import org.zalando.nakadi.validation.schema.ForbiddenAttributeIncompatibility;
import org.zalando.nakadi.validation.schema.SchemaChangeIncompatibility;
import org.zalando.nakadi.validation.schema.SchemaEvolutionConstraint;
import org.zalando.nakadi.validation.schema.SchemaEvolutionIncompatibility;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Stack;
import java.util.stream.Collectors;

import static org.everit.json.schema.CombinedSchema.ALL_CRITERION;
import static org.everit.json.schema.CombinedSchema.ANY_CRITERION;
import static org.everit.json.schema.CombinedSchema.ValidationCriterion;

public class SchemaEvolutionService {

    private final List<SchemaEvolutionConstraint> schemaEvolutionConstraints;
    private final Schema metaSchema;

    public SchemaEvolutionService(final Schema metaSchema,
                                  final List<SchemaEvolutionConstraint> schemaEvolutionConstraints) {
        this.metaSchema = metaSchema;
        this.schemaEvolutionConstraints = schemaEvolutionConstraints;
    }

    public List<SchemaIncompatibility> checkConstraints(final JSONObject schemaJson) {
        final List<SchemaIncompatibility> incompatibilities = new ArrayList<>();

        try {
            metaSchema.validate(schemaJson);
        } catch (final ValidationException e) {
            collectErrorMessages(e, incompatibilities);
        } finally {
            return incompatibilities;
        }
    }

    private void collectErrorMessages(final ValidationException e,
                                      final List<SchemaIncompatibility> incompatibilities) {
        if (e.getCausingExceptions().isEmpty()) {
            incompatibilities.add(
                    new ForbiddenAttributeIncompatibility(e.getPointerToViolation(), e.getErrorMessage()));
        } else {
            e.getCausingExceptions().stream()
                    .forEach(causingException -> {
                        collectErrorMessages(causingException, incompatibilities);
                    });
        }
    }

    public EventType evolve(final EventType original, final EventTypeBase eventType) throws InvalidEventTypeException {
        final Optional<SchemaEvolutionIncompatibility> incompatibility = schemaEvolutionConstraints.stream()
                .map(c -> c.validate(original, eventType)).filter(Optional::isPresent).findFirst()
                .orElse(Optional.empty());

        if (incompatibility.isPresent()) {
            throw new InvalidEventTypeException(incompatibility.get().getReason());
        } else {
            final List<SchemaIncompatibility> incompatibilities = this.checkConstraints(schema(original),
                    schema(eventType));
            if (!incompatibilities.isEmpty()) {
                final String errorMessage = incompatibilities.stream().map(Object::toString)
                        .collect(Collectors.joining(", "));
                throw new InvalidEventTypeException("Invalid schema: " + errorMessage);
            }
            return this.bumpVersion(original, eventType);
        }
    }

    private Schema schema(final EventTypeBase eventType) {
        final JSONObject schemaAsJson = new JSONObject(eventType.getSchema().getSchema());

        return SchemaLoader.load(schemaAsJson);
    }

    private EventType bumpVersion(final EventType original, final EventTypeBase eventType) {
        final DateTime now = new DateTime(DateTimeZone.UTC);
        // TODO: implement PATCH changes
        if (!original.getSchema().getSchema().equals(eventType.getSchema().getSchema())) {
            return new EventType(eventType, original.getSchema().getVersion().bumpMinor().toString(),
                    original.getCreatedAt(), now);
        } else {
            return new EventType(eventType, "1.0.0", original.getCreatedAt(), now);
        }
    }

    public List<SchemaIncompatibility> checkConstraints(final Schema original, final Schema update) {
        final List<SchemaIncompatibility> incompatibilities = new ArrayList<>();

        recursiveCheck(original, update, new Stack<>(), incompatibilities);

        return incompatibilities;
    }

    private void recursiveCheck(
            final Schema original,
            final Schema update,
            final Stack<String> jsonPath,
            final List<SchemaIncompatibility> incompatibilities) {

        if (original == null && update == null) {
            return;
        }

        if (original == null || update == null) {
            addIncompatibleChange(jsonPath, incompatibilities);
            return;
        }

        if (!original.getClass().equals(update.getClass())) {
            addIncompatibleChange("schema types must be the same", jsonPath, incompatibilities);
            return;
        }

        if (original instanceof StringSchema) {
            recursiveCheck((StringSchema) original, (StringSchema) update, jsonPath, incompatibilities);
        } else if (original instanceof NumberSchema) {
            recursiveCheck((NumberSchema) original, (NumberSchema) update, jsonPath, incompatibilities);
        } else if (original instanceof EnumSchema) {
            recursiveCheck((EnumSchema) original, (EnumSchema) update, jsonPath, incompatibilities);
        } else if (original instanceof CombinedSchema) {
            recursiveCheck((CombinedSchema) original, (CombinedSchema) update, jsonPath, incompatibilities);
        } else if (original instanceof ObjectSchema) {
            recursiveCheck((ObjectSchema) original, (ObjectSchema) update, jsonPath, incompatibilities);
        } else if (original instanceof ArraySchema) {
            recursiveCheck((ArraySchema) original, (ArraySchema) update, jsonPath, incompatibilities);
        } else if (original instanceof ReferenceSchema) {
            recursiveCheck((ReferenceSchema) original, (ReferenceSchema) update, jsonPath, incompatibilities);
        }
    }

    private void addIncompatibleChange(final Stack<String> jsonPath,
                                       final List<SchemaIncompatibility> schemaIncompatibilities) {
        addIncompatibleChange("change not allowed", jsonPath, schemaIncompatibilities);
    }

    private void addIncompatibleChange(final String reason, final Stack<String> jsonPath,
                                       final List<SchemaIncompatibility> schemaIncompatibilities) {
        schemaIncompatibilities.add(new SchemaChangeIncompatibility(reason, jsonPathString(jsonPath)));
    }

    private void recursiveCheck(final StringSchema stringSchemaOriginal,
                                final StringSchema stringSchemaUpdate, final Stack<String> jsonPath,
                                final List<SchemaIncompatibility> schemaIncompatibilities) {
        if (stringSchemaOriginal.getMaxLength() != stringSchemaUpdate.getMaxLength()) {
            jsonPath.push("maxLength");
            addIncompatibleChange(jsonPath, schemaIncompatibilities);
            jsonPath.pop();
        } else if (stringSchemaOriginal.getMinLength() != stringSchemaUpdate.getMinLength()) {
            jsonPath.push("minLength");
            addIncompatibleChange(jsonPath, schemaIncompatibilities);
            jsonPath.pop();
        } else if (stringSchemaOriginal.getPattern() != stringSchemaUpdate.getPattern()) {
            jsonPath.push("pattern");
            addIncompatibleChange(jsonPath, schemaIncompatibilities);
            jsonPath.pop();
        }
    }

    private void recursiveCheck(final NumberSchema numberSchemaOriginal, final NumberSchema numberSchemaUpdate,
                                final Stack<String> jsonPath,
                                final List<SchemaIncompatibility> schemaIncompatibilities) {
        if (numberSchemaOriginal.getMaximum() != numberSchemaUpdate.getMaximum()) {
            jsonPath.push("maximum");
            addIncompatibleChange(jsonPath, schemaIncompatibilities);
            jsonPath.pop();
        } else if (numberSchemaOriginal.getMinimum() != numberSchemaUpdate.getMinimum()) {
            jsonPath.push("minimum");
            addIncompatibleChange(jsonPath, schemaIncompatibilities);
            jsonPath.pop();
        } else if (numberSchemaOriginal.getMultipleOf() != numberSchemaUpdate.getMultipleOf()) {
            jsonPath.push("multipleOf");
            addIncompatibleChange(jsonPath, schemaIncompatibilities);
            jsonPath.pop();
        }
    }

    private void recursiveCheck(final EnumSchema enumSchemaOriginal, final EnumSchema enumSchemaUpdate,
                                final Stack<String> jsonPath,
                                final List<SchemaIncompatibility> incompatibilities) {
        if (!enumSchemaOriginal.getPossibleValues().equals(enumSchemaUpdate.getPossibleValues())) {
            jsonPath.push("enum");
            addIncompatibleChange(jsonPath, incompatibilities);
            jsonPath.pop();
        }
    }

    private void recursiveCheck(final ReferenceSchema referenceSchemaOriginal,
                                final ReferenceSchema referenceSchemaUpdate, final Stack<String> jsonPath,
                                final List<SchemaIncompatibility> incompatibilities) {
        jsonPath.push("$ref");
        recursiveCheck(referenceSchemaOriginal.getReferredSchema(), referenceSchemaUpdate.getReferredSchema(),
                jsonPath, incompatibilities);
        jsonPath.pop();
    }

    private void recursiveCheck(final ArraySchema original, final ArraySchema update, final Stack<String> jsonPath,
                                final List<SchemaIncompatibility> incompatibilities) {
        jsonPath.push("items");
        recursiveCheck(original.getAllItemSchema(), update.getAllItemSchema(), jsonPath, incompatibilities);
        jsonPath.pop();

        if ((original.getItemSchemas() != null && update.getItemSchemas() == null)
                || (original.getItemSchemas() == null && update.getItemSchemas() != null)) {
            addIncompatibleChange("the number of schema items cannot be changed", jsonPath, incompatibilities);
        } else if (original.getItemSchemas() != null && update.getItemSchemas() != null) {
            if (original.getItemSchemas().size() != update.getItemSchemas().size()) {
                addIncompatibleChange("the number of schema items cannot be changed", jsonPath, incompatibilities);
            } else {
                final Iterator<Schema> originalIterator = original.getItemSchemas().iterator();
                final Iterator<Schema> updateIterator = update.getItemSchemas().iterator();
                int index = 0;
                while (originalIterator.hasNext()) {
                    jsonPath.push("items/" + index);
                    recursiveCheck(originalIterator.next(), updateIterator.next(), jsonPath, incompatibilities);
                    jsonPath.pop();
                    index += 1;
                }
            }
        }

        if (original.getMaxItems() != update.getMaxItems()) {
            jsonPath.push("maxItems");
            addIncompatibleChange(jsonPath, incompatibilities);
            jsonPath.pop();
        }

        if (original.getMinItems() != update.getMinItems()) {
            jsonPath.push("minItems");
            addIncompatibleChange(jsonPath, incompatibilities);
            jsonPath.pop();
        }

        if (original.needsUniqueItems() != update.needsUniqueItems()) {
            jsonPath.push("uniqueItems");
            addIncompatibleChange(jsonPath, incompatibilities);
            jsonPath.pop();
        }
    }

    private void recursiveCheck(final ObjectSchema original, final ObjectSchema update,
                                final Stack<String> jsonPath,
                                final List<SchemaIncompatibility> incompatibilities) {
        jsonPath.push("properties");
        for (final Map.Entry<String, Schema> property : original.getPropertySchemas().entrySet()) {
            jsonPath.push(property.getKey());
            if (!update.getPropertySchemas().containsKey(property.getKey())) {
                addIncompatibleChange("schema properties cannot be removed", jsonPath, incompatibilities);
            } else {
                recursiveCheck(property.getValue(), update.getPropertySchemas().get(property.getKey()), jsonPath,
                        incompatibilities);
            }
            jsonPath.pop();
        }
        jsonPath.pop();

        jsonPath.push("dependencies");
        for (final Map.Entry<String, Set<String>> dependency : original.getPropertyDependencies().entrySet()) {
            jsonPath.push(dependency.getKey());
            if (!update.getPropertyDependencies().containsKey(dependency.getKey())) {
                addIncompatibleChange("schema dependencies array cannot be changed", jsonPath, incompatibilities);
            } else if (!(dependency.getValue().containsAll(update.getPropertyDependencies().get(dependency.getKey()))
                    && update.getPropertyDependencies().get(dependency.getKey()).containsAll(dependency.getValue()))) {
                addIncompatibleChange("schema dependencies array cannot be changed", jsonPath, incompatibilities);
            }
            jsonPath.pop();
        }

        final List<String> originalDependencies = original.getSchemaDependencies().entrySet().stream()
                .map(Map.Entry::getKey).collect(Collectors.toList());
        final List<String> updateDependencies = update.getSchemaDependencies().entrySet().stream()
                .map(Map.Entry::getKey).collect(Collectors.toList());
        if (!(originalDependencies.containsAll(updateDependencies)
                && updateDependencies.containsAll(originalDependencies))) {
            addIncompatibleChange("schema dependencies cannot be changed", jsonPath, incompatibilities);
        } else {
            for (final Map.Entry<String, Schema> dependency : original.getSchemaDependencies().entrySet()) {
                jsonPath.push(dependency.getKey());
                if (!update.getSchemaDependencies().containsKey(dependency.getKey())) {
                    addIncompatibleChange("schema dependencies cannot be changed", jsonPath, incompatibilities);
                } else {
                    recursiveCheck(dependency.getValue(), update.getSchemaDependencies().get(dependency.getKey()),
                            jsonPath, incompatibilities);
                }
                jsonPath.pop();
            }
        }
        jsonPath.pop();

        if (!(original.getRequiredProperties().containsAll(update.getRequiredProperties())
                && update.getRequiredProperties().containsAll(original.getRequiredProperties()))) {
            jsonPath.push("required");
            addIncompatibleChange(jsonPath, incompatibilities);
            jsonPath.pop();
        }

        if (original.getMaxProperties() != update.getMaxProperties()) {
            jsonPath.push("maxProperties");
            addIncompatibleChange(jsonPath, incompatibilities);
            jsonPath.pop();
        }

        if (original.getMinProperties() != update.getMinProperties()) {
            jsonPath.push("minProperties");
            addIncompatibleChange(jsonPath, incompatibilities);
            jsonPath.pop();
        }
    }

    private void recursiveCheck(final CombinedSchema combinedSchemaOriginal, final CombinedSchema combinedSchemaUpdate,
                                final Stack<String> jsonPath,
                                final List<SchemaIncompatibility> incompatibilities) {
        if(!(combinedSchemaOriginal.getSubschemas().containsAll(combinedSchemaUpdate.getSubschemas())
                && combinedSchemaUpdate.getSubschemas().containsAll(combinedSchemaOriginal.getSubschemas()))) {
            addIncompatibleChange("sub schemas must not be changed", jsonPath, incompatibilities);
        } else {
            if (!combinedSchemaOriginal.getCriterion().equals(combinedSchemaUpdate.getCriterion())) {
                incompatibilities.add(new SchemaChangeIncompatibility("the validation criteria must not be " +
                        "changed from " + validationCriteria(combinedSchemaOriginal.getCriterion()) + " to " +
                        validationCriteria(combinedSchemaUpdate.getCriterion()), jsonPathString(jsonPath)));
            } else {
                final Iterator<Schema> originalIterator = combinedSchemaOriginal.getSubschemas().iterator();
                final Iterator<Schema> updateIterator = combinedSchemaUpdate.getSubschemas().iterator();
                int index = 0;
                while (originalIterator.hasNext()) {
                    jsonPath.push(validationCriteria(combinedSchemaOriginal.getCriterion()) + "/" + index);
                    recursiveCheck(originalIterator.next(), updateIterator.next(), jsonPath, incompatibilities);
                    jsonPath.pop();
                    index += 1;
                }
            }
        }
    }

    private String validationCriteria(final ValidationCriterion criterion) {
        if (criterion.equals(ALL_CRITERION)) {
            return "allOf";
        } else if (criterion.equals(ANY_CRITERION)) {
            return "anyOf";
        } else {
            return "oneOf";
        }
    }

    private String jsonPathString(final List<String> jsonPath) {
        return "#/" + String.join("/", jsonPath);
    }
}
