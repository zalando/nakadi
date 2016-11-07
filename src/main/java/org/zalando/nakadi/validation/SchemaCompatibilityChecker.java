package org.zalando.nakadi.validation;

import org.everit.json.schema.ArraySchema;
import org.everit.json.schema.CombinedSchema;
import org.everit.json.schema.ObjectSchema;
import org.everit.json.schema.ReferenceSchema;
import org.everit.json.schema.Schema;
import org.zalando.nakadi.validation.schema.SchemaConstraint;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Stack;

public class SchemaCompatibilityChecker {

    private final List<SchemaConstraint> constraints;

    public SchemaCompatibilityChecker(final List<SchemaConstraint> constraints) {
        this.constraints = constraints;
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

        for (final SchemaConstraint constraint : constraints) {
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
}
