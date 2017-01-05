package org.zalando.nakadi.validation.schema.diff;

import org.everit.json.schema.NumberSchema;
import org.zalando.nakadi.domain.SchemaChange;

import java.util.List;
import java.util.Objects;
import java.util.Stack;

import static org.zalando.nakadi.domain.SchemaChange.Type.ATTRIBUTE_VALUE_CHANGED;

class NumberSchemaDiff {
    static void recursiveCheck(final NumberSchema numberSchemaOriginal, final NumberSchema numberSchemaUpdate,
                               final Stack<String> jsonPath, final List<SchemaChange> changes) {
        if (!Objects.equals(numberSchemaOriginal.getMaximum(), numberSchemaUpdate.getMaximum())) {
            SchemaDiff.addChange("maximum", ATTRIBUTE_VALUE_CHANGED, jsonPath, changes);
        } else if (!Objects.equals(numberSchemaOriginal.getMinimum(), numberSchemaUpdate.getMinimum())) {
            SchemaDiff.addChange("minimum", ATTRIBUTE_VALUE_CHANGED, jsonPath, changes);
        } else if (!Objects.equals(numberSchemaOriginal.getMultipleOf(), numberSchemaUpdate.getMultipleOf())) {
            SchemaDiff.addChange("multipleOf", ATTRIBUTE_VALUE_CHANGED, jsonPath, changes);
        }
    }
}
