package org.zalando.nakadi.validation.schema;

import org.everit.json.schema.ObjectSchema;
import org.everit.json.schema.Schema;
import org.zalando.nakadi.validation.SchemaIncompatibility;

import java.util.List;
import java.util.Optional;

public class AdditionalPropertiesConstraint extends SchemaConstraint {
    @Override
    public Optional<SchemaIncompatibility> validate(final List<String> jsonPath, final Schema schema) {
        if (schema instanceof ObjectSchema) {
            if (((ObjectSchema) schema).permitsAdditionalProperties()) {
                return Optional.of(new ForbiddenAttributeIncompatibility(jsonPathString(jsonPath), "additionalProperties"));
            }
        }
        return Optional.empty();
    }
}
