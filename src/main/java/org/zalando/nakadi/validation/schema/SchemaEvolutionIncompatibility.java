package org.zalando.nakadi.validation.schema;

public class SchemaEvolutionIncompatibility {
    private final String reason;

    public SchemaEvolutionIncompatibility(final String reason) {
        this.reason = reason;
    }

    public String getReason() {
        return reason;
    }

    public static class CategoryIncompatibility extends SchemaEvolutionIncompatibility {
        public CategoryIncompatibility(final String reason) {
            super(reason);
        }
    }

    public static class MetadataIncompatibility extends SchemaEvolutionIncompatibility {
        public MetadataIncompatibility(final String reason) {
            super(reason);
        }
    }
}
