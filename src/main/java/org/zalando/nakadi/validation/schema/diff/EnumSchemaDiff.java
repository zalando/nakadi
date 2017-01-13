package org.zalando.nakadi.validation.schema.diff;

import org.everit.json.schema.EnumSchema;
import org.zalando.nakadi.domain.SchemaChange;

import java.util.List;
import java.util.Stack;

import static org.zalando.nakadi.domain.SchemaChange.Type.ENUM_ARRAY_CHANGED;

class EnumSchemaDiff {
    static void recursiveCheck(final EnumSchema enumSchemaOriginal, final EnumSchema enumSchemaUpdate,
                                       final Stack<String> jsonPath, final List<SchemaChange> changes) {
        if (!enumSchemaOriginal.getPossibleValues().equals(enumSchemaUpdate.getPossibleValues())) {
            SchemaDiff.addChange("enum", ENUM_ARRAY_CHANGED, jsonPath, changes);
        }
    }
}
