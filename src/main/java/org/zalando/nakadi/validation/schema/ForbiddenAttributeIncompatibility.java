package org.zalando.nakadi.validation.schema;

import org.zalando.nakadi.validation.SchemaIncompatibility;

public class ForbiddenAttributeIncompatibility extends SchemaIncompatibility {
    private final String attribute;

    public ForbiddenAttributeIncompatibility(final String jsonPath, final String attribute) {
        super(jsonPath);
        this.attribute = attribute;
    }

    public String getAttribute() {
        return attribute;
    }

    @Override
    public String toString() {
        return "Forbidden attribute \"" + this.attribute + "\" found in " + this.getJsonPath();
    }
}
