package org.zalando.nakadi.validation.schema.diff;

import org.everit.json.schema.NumberSchema;

import java.util.Objects;

import static org.zalando.nakadi.domain.SchemaChange.Type.ATTRIBUTE_VALUE_CHANGED;

class NumberSchemaDiff {
    static void recursiveCheck(final NumberSchema numberSchemaOriginal, final NumberSchema numberSchemaUpdate,
                               final SchemaDiffState state) {
        if (!Objects.equals(numberSchemaOriginal.getMaximum(), numberSchemaUpdate.getMaximum())) {
            state.addChange("maximum", ATTRIBUTE_VALUE_CHANGED);
        } else if (!Objects.equals(numberSchemaOriginal.getMinimum(), numberSchemaUpdate.getMinimum())) {
            state.addChange("minimum", ATTRIBUTE_VALUE_CHANGED);
        } else if (!Objects.equals(numberSchemaOriginal.getMultipleOf(), numberSchemaUpdate.getMultipleOf())) {
            state.addChange("multipleOf", ATTRIBUTE_VALUE_CHANGED);
        }
    }
}
