package org.zalando.nakadi.validation;

public class SchemaIncompatibility {
    private final String jsonPath;

    public SchemaIncompatibility(final String jsonPath) {
        this.jsonPath = jsonPath;
    }

    public String getJsonPath() {
        return jsonPath;
    }
}
