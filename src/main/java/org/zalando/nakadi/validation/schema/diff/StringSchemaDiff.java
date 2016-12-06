package org.zalando.nakadi.validation.schema.diff;

import org.everit.json.schema.StringSchema;
import org.zalando.nakadi.domain.SchemaChange;

import java.util.List;
import java.util.Objects;
import java.util.Stack;

import static org.zalando.nakadi.domain.SchemaChange.Type.ATTRIBUTE_VALUE_CHANGED;

class StringSchemaDiff extends SchemaDiff {
    static void recursiveCheck(final StringSchema stringSchemaOriginal, final StringSchema stringSchemaUpdate,
                               final Stack<String> jsonPath, final List<SchemaChange> changes) {
        if (!Objects.equals(stringSchemaOriginal.getMaxLength(), stringSchemaUpdate.getMaxLength())) {
            addChange("maxLength", ATTRIBUTE_VALUE_CHANGED, jsonPath, changes);
        } else if (!Objects.equals(stringSchemaOriginal.getMinLength(), stringSchemaUpdate.getMinLength())) {
            addChange("minLength", ATTRIBUTE_VALUE_CHANGED, jsonPath, changes);
        } else if (stringSchemaOriginal.getPattern() != stringSchemaUpdate.getPattern()) {
            addChange("pattern", ATTRIBUTE_VALUE_CHANGED, jsonPath, changes);
        }
    }
}
