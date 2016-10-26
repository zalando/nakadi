package org.zalando.nakadi.validation.schema;

import org.everit.json.schema.loader.internal.JSONPointer;
import org.zalando.nakadi.validation.SchemaIncompatibility;

import java.util.List;

public class ForbiddenAttributeIncompatibility extends SchemaIncompatibility {
    final private String attribute;

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
