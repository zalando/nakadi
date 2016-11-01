package org.zalando.nakadi.validation.schema;

import org.everit.json.schema.NotSchema;
import org.everit.json.schema.Schema;
import org.zalando.nakadi.validation.SchemaIncompatibility;

import java.util.List;
import java.util.Optional;

public class NotSchemaConstraint implements SchemaConstraint {
    @Override
    public Optional<SchemaIncompatibility> validate(final List<String> jsonPath, final Schema schema) {
        if (schema instanceof NotSchema) {
            return Optional.of(new ForbiddenAttributeIncompatibility(jsonPathString(jsonPath), "not"));
        } else {
            return Optional.empty();
        }
    }

    private String jsonPathString(final List<String> jsonPath) {
        return "#/" + String.join("/", jsonPath);
    }
}
