package org.zalando.nakadi.validation.schema;

public class SchemaEvolutionIncompatibility {
    private final String reason;

    public SchemaEvolutionIncompatibility(final String reason) {
        this.reason = reason;
    }

    public String getReason() {
        return reason;
    }
}
