package org.zalando.nakadi.validation.schema.diff;

import org.everit.json.schema.StringSchema;
import org.zalando.nakadi.domain.SchemaChange;

import java.util.List;
import java.util.Objects;
import java.util.Stack;

import static org.zalando.nakadi.domain.SchemaChange.Type.ATTRIBUTE_VALUE_CHANGED;
import static org.zalando.nakadi.validation.schema.diff.SchemaDiff.addChange;

class StringSchemaDiff {
    static void recursiveCheck(final StringSchema stringSchemaOriginal, final StringSchema stringSchemaUpdate,
                               final Stack<String> jsonPath, final List<SchemaChange> changes) {
        if (!Objects.equals(stringSchemaOriginal.getMaxLength(), stringSchemaUpdate.getMaxLength())) {
            addChange("maxLength", ATTRIBUTE_VALUE_CHANGED, jsonPath, changes);
        } else if (!Objects.equals(stringSchemaOriginal.getMinLength(), stringSchemaUpdate.getMinLength())) {
            addChange("minLength", ATTRIBUTE_VALUE_CHANGED, jsonPath, changes);
        } else if (stringSchemaOriginal.getPattern() != null && stringSchemaUpdate.getPattern() != null
                && !stringSchemaOriginal.getPattern().pattern().equals(stringSchemaUpdate.getPattern().pattern())) {
            addChange("pattern", ATTRIBUTE_VALUE_CHANGED, jsonPath, changes);
        }
    }
}
