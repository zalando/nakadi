package org.zalando.nakadi.validation.schema.diff;

import org.everit.json.schema.EnumSchema;

import static org.zalando.nakadi.domain.SchemaChange.Type.ENUM_ARRAY_CHANGED;

class EnumSchemaDiff {
    static void recursiveCheck(
            final EnumSchema enumSchemaOriginal, final EnumSchema enumSchemaUpdate, final SchemaDiffState state) {
        if (!enumSchemaOriginal.getPossibleValues().equals(enumSchemaUpdate.getPossibleValues())) {
            state.addChange("enum", ENUM_ARRAY_CHANGED);
        }
    }
}
