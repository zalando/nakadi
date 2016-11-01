package org.zalando.nakadi.validation.schema;

import org.everit.json.schema.ObjectSchema;
import org.everit.json.schema.Schema;
import org.zalando.nakadi.validation.SchemaIncompatibility;

import java.util.List;
import java.util.Optional;

public class PatternPropertiesConstraint implements SchemaConstraint {
    private final String attribute;

    public PatternPropertiesConstraint(final String attribute) {
        this.attribute = attribute;
    }

    @Override
    public Optional<SchemaIncompatibility> validate(final List<String> jsonPath, final Schema schema) {
        if (schema instanceof ObjectSchema) {
            final ObjectSchema objectSchema = (ObjectSchema) schema;
            if (!objectSchema.getPatternProperties().isEmpty()) {
                return Optional.of(new ForbiddenAttributeIncompatibility(jsonPathString(jsonPath), this.attribute));
            }
        }
        return Optional.empty();
    }

    private String jsonPathString(final List<String> jsonPath) {
        return "#/" + String.join("/", jsonPath);
    }
}
