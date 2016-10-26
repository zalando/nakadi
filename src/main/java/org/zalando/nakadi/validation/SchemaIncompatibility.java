package org.zalando.nakadi.validation;

import java.util.List;

public class SchemaIncompatibility {
    final private String jsonPath;

    public SchemaIncompatibility(final String jsonPath) {
        this.jsonPath = jsonPath;
    }

    public String getJsonPath() {
        return jsonPath;
    }
}
