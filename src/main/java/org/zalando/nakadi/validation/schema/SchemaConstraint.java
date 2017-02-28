package org.zalando.nakadi.validation.schema;

import java.util.List;
import java.util.Optional;
import org.everit.json.schema.Schema;
import org.zalando.nakadi.validation.SchemaIncompatibility;

public abstract class SchemaConstraint {
    public abstract Optional<SchemaIncompatibility> validate(List<String> jsonPath, Schema schema);

    protected String jsonPathString(final List<String> jsonPath) {
        return "#/" + String.join("/", jsonPath);
    }
}
