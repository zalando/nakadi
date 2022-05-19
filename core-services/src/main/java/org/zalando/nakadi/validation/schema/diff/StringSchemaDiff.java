package org.zalando.nakadi.validation.schema.diff;

import org.everit.json.schema.StringSchema;

import java.util.Objects;

import static org.zalando.nakadi.domain.SchemaChange.Type.ATTRIBUTE_VALUE_CHANGED;

class StringSchemaDiff {
    static void recursiveCheck(final StringSchema stringSchemaOriginal, final StringSchema stringSchemaUpdate,
                               final SchemaDiffState state) {
        if (!Objects.equals(stringSchemaOriginal.getMaxLength(), stringSchemaUpdate.getMaxLength())) {
            state.addChange("maxLength", ATTRIBUTE_VALUE_CHANGED);
        } else if (!Objects.equals(stringSchemaOriginal.getMinLength(), stringSchemaUpdate.getMinLength())) {
            state.addChange("minLength", ATTRIBUTE_VALUE_CHANGED);
        } else if (stringSchemaOriginal.getPattern() != null && stringSchemaUpdate.getPattern() != null
                && !stringSchemaOriginal.getPattern().pattern().equals(stringSchemaUpdate.getPattern().pattern())) {
            state.addChange("pattern", ATTRIBUTE_VALUE_CHANGED);
        }
    }
}
