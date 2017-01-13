package org.zalando.nakadi.validation.schema;

import org.everit.json.schema.Schema;
import org.zalando.nakadi.validation.SchemaIncompatibility;

import java.util.List;
import java.util.Optional;

public abstract class SchemaConstraint {
    public abstract Optional<SchemaIncompatibility> validate(final List<String> jsonPath, final Schema schema);

    protected String jsonPathString(final List<String> jsonPath) {
        return "#/" + String.join("/", jsonPath);
    }
}
