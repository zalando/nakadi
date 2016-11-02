package org.zalando.nakadi.validation.schema;

import org.everit.json.schema.ObjectSchema;
import org.everit.json.schema.Schema;
import org.zalando.nakadi.validation.SchemaIncompatibility;

import java.util.List;
import java.util.Optional;

public class PatternPropertiesConstraint extends SchemaConstraint {

    static final String ATTRIBUTE = "patternProperties";

    @Override
    public Optional<SchemaIncompatibility> validate(final List<String> jsonPath, final Schema schema) {
        if (schema instanceof ObjectSchema) {
            final ObjectSchema objectSchema = (ObjectSchema) schema;
            if (!objectSchema.getPatternProperties().isEmpty()) {
                return Optional.of(new ForbiddenAttributeIncompatibility(jsonPathString(jsonPath), this.ATTRIBUTE));
            }
        }
        return Optional.empty();
    }
}
