package org.zalando.nakadi.validation.schema;

import org.zalando.nakadi.validation.SchemaIncompatibility;

public class SchemaChangeIncompatibility extends SchemaIncompatibility {
    private final String reason;

    public SchemaChangeIncompatibility(final String reason, final String jsonPath) {
        super(jsonPath);
        this.reason = reason;
    }

    @Override
    public String toString() {
        return "Incompatibility found in \"" + this.getJsonPath() + "\": " + this.reason;
    }
}
