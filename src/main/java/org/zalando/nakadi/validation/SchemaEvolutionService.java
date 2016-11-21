package org.zalando.nakadi.validation;

import org.everit.json.schema.ArraySchema;
import org.everit.json.schema.CombinedSchema;
import org.everit.json.schema.ObjectSchema;
import org.everit.json.schema.ReferenceSchema;
import org.everit.json.schema.Schema;
import org.everit.json.schema.loader.SchemaLoader;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.json.JSONObject;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.EventTypeBase;
import org.zalando.nakadi.exceptions.InvalidEventTypeException;
import org.zalando.nakadi.validation.schema.SchemaChangeIncompatibility;
import org.zalando.nakadi.validation.schema.SchemaConstraint;
import org.zalando.nakadi.validation.schema.SchemaEvolutionConstraint;
import org.zalando.nakadi.validation.schema.SchemaEvolutionIncompatibility;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Stack;
import java.util.stream.Collectors;

import static org.everit.json.schema.CombinedSchema.*;

public class SchemaEvolutionService {

    private final List<SchemaConstraint> jsonSchemaConstraints;
    private final List<SchemaEvolutionConstraint> schemaEvolutionConstraints;

    public SchemaEvolutionService(final List<SchemaConstraint> jsonSchemaConstraints,
                                  final List<SchemaEvolutionConstraint> schemaEvolutionConstraints) {
        this.jsonSchemaConstraints = jsonSchemaConstraints;
        this.schemaEvolutionConstraints = schemaEvolutionConstraints;
    }

    public List<SchemaIncompatibility> checkConstraints(final Schema schema) {
        final List<SchemaIncompatibility> incompatibilities = new ArrayList<>();

        recursiveCheckConstraints(schema, new Stack<>(), incompatibilities);

        return incompatibilities;
    }

    private void recursiveCheckConstraints(
            final Schema schema,
            final Stack<String> jsonPath,
            final List<SchemaIncompatibility> schemaIncompatibilities) {

        for (final SchemaConstraint constraint : jsonSchemaConstraints) {
            final Optional<SchemaIncompatibility> incompatibility = constraint.validate(jsonPath, schema);
            if (incompatibility.isPresent()) {
                schemaIncompatibilities.add(incompatibility.get());
            }
        }

        if (schema instanceof ObjectSchema) {
            recursiveCheckConstraints((ObjectSchema) schema, jsonPath, schemaIncompatibilities);
        } else if (schema instanceof ArraySchema) {
            recursiveCheckConstraints((ArraySchema) schema, jsonPath, schemaIncompatibilities);
        } else if (schema instanceof ReferenceSchema) {
            recursiveCheckConstraints((ReferenceSchema) schema, jsonPath, schemaIncompatibilities);
        } else if (schema instanceof CombinedSchema) {
            recursiveCheckConstraints((CombinedSchema) schema, jsonPath, schemaIncompatibilities);
        }
    }

    private void recursiveCheckConstraints(final CombinedSchema schema, final Stack<String> jsonPath,
                                           final List<SchemaIncompatibility> schemaIncompatibilities) {
        if (!schema.getSubschemas().isEmpty()) {
            for (final Schema innerSchema : schema.getSubschemas()) {
                recursiveCheckConstraints(innerSchema, jsonPath, schemaIncompatibilities);
            }
        }
    }

    private void recursiveCheckConstraints(final ReferenceSchema schema, final Stack<String> jsonPath,
                                           final List<SchemaIncompatibility> schemaIncompatibilities) {
        if (schema.getReferredSchema() != null) {
            recursiveCheckConstraints(schema.getReferredSchema(), jsonPath, schemaIncompatibilities);
        }
    }

    private void recursiveCheckConstraints(final ArraySchema schema, final Stack<String> jsonPath,
                                           final List<SchemaIncompatibility> schemaIncompatibilities) {
        if (schema.getItemSchemas() != null) {
            jsonPath.push("items");
            for (final Schema innerSchema : schema.getItemSchemas()) {
                recursiveCheckConstraints(innerSchema, jsonPath, schemaIncompatibilities);
            }
            jsonPath.pop();
        }
        if(schema.getAllItemSchema() != null) {
            jsonPath.push("items");
            recursiveCheckConstraints(schema.getAllItemSchema(), jsonPath, schemaIncompatibilities);
            jsonPath.pop();
        }
        if(schema.getSchemaOfAdditionalItems() != null) {
            jsonPath.push("additionalItems");
            recursiveCheckConstraints(schema.getSchemaOfAdditionalItems(), jsonPath, schemaIncompatibilities);
            jsonPath.pop();
        }
    }

    private void recursiveCheckConstraints(final ObjectSchema schema, final Stack<String> jsonPath,
                                           final List<SchemaIncompatibility> schemaIncompatibilities) {
        jsonPath.push("properties");
        for (final Map.Entry<String, Schema> innerSchema : schema.getPropertySchemas().entrySet()) {
            jsonPath.push(innerSchema.getKey());
            recursiveCheckConstraints(innerSchema.getValue(), jsonPath, schemaIncompatibilities);
            jsonPath.pop();
        }
        jsonPath.pop();
        jsonPath.push("dependencies");
        for (final Map.Entry<String, Schema> innerSchema : schema.getSchemaDependencies().entrySet()) {
            jsonPath.push(innerSchema.getKey());
            recursiveCheckConstraints(innerSchema.getValue(), jsonPath, schemaIncompatibilities);
            jsonPath.pop();
        }
        jsonPath.pop();
        jsonPath.push("additionalProperties");
        if (schema.getSchemaOfAdditionalProperties() != null) {
            recursiveCheckConstraints(schema.getSchemaOfAdditionalProperties(), jsonPath, schemaIncompatibilities);
        }
        jsonPath.pop();
    }

    public EventType evolve(final EventType original, final EventTypeBase eventType) throws InvalidEventTypeException {
        final Optional<SchemaEvolutionIncompatibility> incompatibility = schemaEvolutionConstraints.stream()
                .map(c -> c.validate(original, eventType)).filter(Optional::isPresent).findFirst()
                .orElse(Optional.empty());

        if (incompatibility.isPresent()) {
            throw new InvalidEventTypeException(incompatibility.get().getReason());
        } else {
            final List<SchemaIncompatibility> incompatibilities = this.checkConstraints(schema(original), schema(eventType));
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
            return new EventType(eventType, original.getSchema().getVersion().bumpMinor().toString(), original.getCreatedAt(), now);
        } else {
            return new EventType(eventType, "1.0.0", original.getCreatedAt(), now);
        }
    }

    public List<SchemaIncompatibility> checkConstraints(final Schema original, final Schema update) {
        final List<SchemaIncompatibility> incompatibilities = new ArrayList<>();

        recursiveCheckConstraints(original, update, new Stack<>(), incompatibilities);

        return incompatibilities;
    }

    private void recursiveCheckConstraints(
            final Schema original,
            final Schema update,
            final Stack<String> jsonPath,
            final List<SchemaIncompatibility> schemaIncompatibilities) {

        if (original == null && update == null) {
            return;
        }

        if (original == null || update == null) {
            schemaIncompatibilities.add(new SchemaChangeIncompatibility("change not allowed", jsonPathString(jsonPath)));
            return;
        }

        if (!original.getClass().equals(update.getClass())) {
            schemaIncompatibilities.add(new SchemaChangeIncompatibility("schema types must be the same", jsonPathString(jsonPath)));
            return;
        }

        if (original instanceof CombinedSchema) {
            recursiveCheckConstraints((CombinedSchema) original, (CombinedSchema) update, jsonPath, schemaIncompatibilities);
        } else if (original instanceof ObjectSchema) {
            recursiveCheckConstraints((ObjectSchema) original, (ObjectSchema) update, jsonPath, schemaIncompatibilities);
        } else if (original instanceof ArraySchema) {
            recursiveCheckConstraints((ArraySchema) original, (ArraySchema) update, jsonPath, schemaIncompatibilities);
        } else if (original instanceof ReferenceSchema) {
            recursiveCheckConstraints((ReferenceSchema) original, (ReferenceSchema) update, jsonPath, schemaIncompatibilities);
        }
    }

    private void recursiveCheckConstraints(final ReferenceSchema referenceSchemaOriginal, final ReferenceSchema referenceSchemaUpdate, final Stack<String> jsonPath, final List<SchemaIncompatibility> schemaIncompatibilities) {
        jsonPath.push("$ref");
        recursiveCheckConstraints(referenceSchemaOriginal.getReferredSchema(), referenceSchemaUpdate.getReferredSchema(), jsonPath, schemaIncompatibilities);
        jsonPath.pop();
    }

    private void recursiveCheckConstraints(final ArraySchema original, final ArraySchema update, final Stack<String> jsonPath, final List<SchemaIncompatibility> schemaIncompatibilities) {
        jsonPath.push("items");
        recursiveCheckConstraints(original.getAllItemSchema(), update.getAllItemSchema(), jsonPath, schemaIncompatibilities);
        jsonPath.pop();

        if ((original.getItemSchemas() != null && update.getItemSchemas() == null) || (original.getItemSchemas() == null && update.getItemSchemas() != null)) {
            schemaIncompatibilities.add(new SchemaChangeIncompatibility("the number of schema items cannot be changed", jsonPathString(jsonPath)));
        } else if (original.getItemSchemas() != null && update.getItemSchemas() != null) {
            if (original.getItemSchemas().size() != update.getItemSchemas().size()) {
                schemaIncompatibilities.add(new SchemaChangeIncompatibility("the number of schema items cannot be changed", jsonPathString(jsonPath)));
            } else {
                final Iterator<Schema> originalIterator = original.getItemSchemas().iterator();
                final Iterator<Schema> updateIterator = update.getItemSchemas().iterator();
                final int index = 0;
                while (originalIterator.hasNext()) {
                    jsonPath.push("items[" + index + "]");
                    recursiveCheckConstraints(originalIterator.next(), updateIterator.next(), jsonPath, schemaIncompatibilities);
                    jsonPath.pop();
                }
            }
        }

        if (original.getMaxItems() != update.getMaxItems()) {
            schemaIncompatibilities.add(new SchemaChangeIncompatibility("maxItems cannot be changed", jsonPathString(jsonPath)));
        }

        if (original.getMinItems() != update.getMinItems()) {
            schemaIncompatibilities.add(new SchemaChangeIncompatibility("minItems cannot be changed", jsonPathString(jsonPath)));
        }

        if (original.needsUniqueItems() != update.needsUniqueItems()) {
            schemaIncompatibilities.add(new SchemaChangeIncompatibility("uniqueItems cannot be changed", jsonPathString(jsonPath)));
        }
    }

    private void recursiveCheckConstraints(final ObjectSchema original, final ObjectSchema update, final Stack<String> jsonPath, final List<SchemaIncompatibility> schemaIncompatibilities) {
        jsonPath.push("properties");
        for (final Map.Entry<String, Schema> property : original.getPropertySchemas().entrySet()) {
            jsonPath.push(property.getKey());
            if (!update.getPropertySchemas().containsKey(property.getKey())) {
                schemaIncompatibilities.add(new SchemaChangeIncompatibility("schema properties cannot be removed", jsonPathString(jsonPath)));
            } else {
                recursiveCheckConstraints(property.getValue(), update.getPropertySchemas().get(property.getKey()), jsonPath, schemaIncompatibilities);
            }
            jsonPath.pop();
        }
        jsonPath.pop();

        jsonPath.push("dependencies");
        for (final Map.Entry<String, Set<String>> dependency : original.getPropertyDependencies().entrySet()) {
            jsonPath.push(dependency.getKey());
            if (!update.getPropertyDependencies().containsKey(dependency.getKey())) {
                schemaIncompatibilities.add(new SchemaChangeIncompatibility("schema dependencies array cannot be changed", jsonPathString(jsonPath)));
            } else if (!(dependency.getValue().containsAll(update.getPropertyDependencies().get(dependency.getKey())) && update.getPropertyDependencies().get(dependency.getKey()).containsAll(dependency.getValue()))) {
                schemaIncompatibilities.add(new SchemaChangeIncompatibility("schema dependencies array cannot be changed", jsonPathString(jsonPath)));
            }
            jsonPath.pop();
        }

        final List<String> originalDependencies = original.getSchemaDependencies().entrySet().stream().map(Map.Entry::getKey).collect(Collectors.toList());
        final List<String> updateDependencies = update.getSchemaDependencies().entrySet().stream().map(Map.Entry::getKey).collect(Collectors.toList());
        if (!(originalDependencies.containsAll(updateDependencies) && updateDependencies.containsAll(originalDependencies))) {
            schemaIncompatibilities.add(new SchemaChangeIncompatibility("schema dependencies cannot be changed", jsonPathString(jsonPath)));
            jsonPath.pop();
        } else {
            for (final Map.Entry<String, Schema> dependency : original.getSchemaDependencies().entrySet()) {
                jsonPath.push(dependency.getKey());
                if (!update.getSchemaDependencies().containsKey(dependency.getKey())) {
                    schemaIncompatibilities.add(new SchemaChangeIncompatibility("schema dependencies cannot be changed", jsonPathString(jsonPath)));
                } else {
                    recursiveCheckConstraints(dependency.getValue(), update.getSchemaDependencies().get(dependency.getKey()), jsonPath, schemaIncompatibilities);
                }
                jsonPath.pop();
            }
            jsonPath.pop();
        }

        if (!(original.getRequiredProperties().containsAll(update.getRequiredProperties()) && update.getRequiredProperties().containsAll(original.getRequiredProperties()))) {
            schemaIncompatibilities.add(new SchemaChangeIncompatibility("required properties cannot be changed", jsonPathString(jsonPath)));
        }

        if (original.getMaxProperties() != update.getMaxProperties()) {
            schemaIncompatibilities.add(new SchemaChangeIncompatibility("maxProperties cannot be changed", jsonPathString(jsonPath)));
        }

        if (original.getMinProperties() != update.getMinProperties()) {
            schemaIncompatibilities.add(new SchemaChangeIncompatibility("minProperties cannot be changed", jsonPathString(jsonPath)));
        }
    }

    private void recursiveCheckConstraints(final CombinedSchema combinedSchemaOriginal, final CombinedSchema combinedSchemaUpdate, final Stack<String> jsonPath, final List<SchemaIncompatibility> schemaIncompatibilities) {
        if(!(combinedSchemaOriginal.getSubschemas().containsAll(combinedSchemaUpdate.getSubschemas()) &&
                combinedSchemaUpdate.getSubschemas().containsAll(combinedSchemaOriginal.getSubschemas()))) {
            schemaIncompatibilities.add(new SchemaChangeIncompatibility("sub schemas must not be changed", jsonPathString(jsonPath)));
        } else {
            if (!combinedSchemaOriginal.getCriterion().equals(combinedSchemaUpdate.getCriterion())) {
                schemaIncompatibilities.add(new SchemaChangeIncompatibility("the validation criteria must not be " +
                        "changed from " + validationCriteria(combinedSchemaOriginal.getCriterion()) + " to " +
                        validationCriteria(combinedSchemaUpdate.getCriterion()), jsonPathString(jsonPath)));
            } else {
                final Iterator<Schema> originalIterator = combinedSchemaOriginal.getSubschemas().iterator();
                final Iterator<Schema> updateIterator = combinedSchemaUpdate.getSubschemas().iterator();
                final int index = 0;
                while (originalIterator.hasNext()) {
                    jsonPath.push(validationCriteria(combinedSchemaOriginal.getCriterion()) + "[" + index + "]");
                    recursiveCheckConstraints(originalIterator.next(), updateIterator.next(), jsonPath, schemaIncompatibilities);
                    jsonPath.pop();
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
