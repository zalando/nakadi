package org.zalando.nakadi.validation.schema;

import org.everit.json.schema.Schema;
import org.zalando.nakadi.validation.SchemaIncompatibility;

import java.util.List;
import java.util.Optional;

public interface SchemaConstraint {
    Optional<SchemaIncompatibility> validate(final List<String> jsonPath, final Schema schema);
}
