package org.zalando.nakadi.validation.schema;

import org.everit.json.schema.ArraySchema;
import org.everit.json.schema.ObjectSchema;
import org.everit.json.schema.Schema;
import org.zalando.nakadi.validation.SchemaIncompatibility;

import java.util.List;
import java.util.Optional;

public class AdditionalItemsConstraint extends SchemaConstraint {
    @Override
    public Optional<SchemaIncompatibility> validate(final List<String> jsonPath, final Schema schema) {
        if (schema instanceof ArraySchema) {
            if (((ArraySchema) schema).permitsAdditionalItems()) {
                return Optional.of(new ForbiddenAttributeIncompatibility(jsonPathString(jsonPath), "additionalItems"));
            }
        }
        return Optional.empty();
    }
}
