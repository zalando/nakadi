package org.zalando.nakadi.validation.schema;

import org.zalando.nakadi.validation.SchemaIncompatibility;

public class ForbiddenAttributeIncompatibility extends SchemaIncompatibility {
    private final String reason;

    public ForbiddenAttributeIncompatibility(final String jsonPath, final String reason) {
        super(jsonPath);
        this.reason = reason;
    }

    @Override
    public String toString() {
        return "Invalid schema found in [" + this.getJsonPath() + "]: " + this.reason;
    }
}
