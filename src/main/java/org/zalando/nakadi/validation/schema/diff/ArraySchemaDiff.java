package org.zalando.nakadi.validation.schema.diff;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import org.everit.json.schema.ArraySchema;
import org.everit.json.schema.Schema;
import org.zalando.nakadi.domain.SchemaChange;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Stack;

import static org.zalando.nakadi.domain.SchemaChange.Type.ADDITIONAL_ITEMS_CHANGED;
import static org.zalando.nakadi.domain.SchemaChange.Type.ATTRIBUTE_VALUE_CHANGED;
import static org.zalando.nakadi.domain.SchemaChange.Type.NUMBER_OF_ITEMS_CHANGED;

public class ArraySchemaDiff {
    static void recursiveCheck(final ArraySchema original, final ArraySchema update, final Stack<String> jsonPath,
                                       final List<SchemaChange> changes) {
        compareItemSchemaObject(original, update, jsonPath, changes);

        compareItemSchemaArray(original, update, jsonPath, changes);

        compareAdditionalItems(original, update, jsonPath, changes);

        compareAttributes(original, update, jsonPath, changes);
    }

    private static void compareAttributes(final ArraySchema original, final ArraySchema update,
                                          final Stack<String> jsonPath, final List<SchemaChange> changes) {
        if (!Objects.equals(original.getMaxItems(), update.getMaxItems())) {
            SchemaDiff.addChange("maxItems", ATTRIBUTE_VALUE_CHANGED, jsonPath, changes);
        }

        if (!Objects.equals(original.getMinItems(), update.getMinItems())) {
            SchemaDiff.addChange("minItems", ATTRIBUTE_VALUE_CHANGED, jsonPath, changes);
        }

        if (original.needsUniqueItems() != update.needsUniqueItems()) {
            SchemaDiff.addChange("uniqueItems", ATTRIBUTE_VALUE_CHANGED, jsonPath, changes);
        }
    }

    private static void compareAdditionalItems(final ArraySchema original, final ArraySchema update,
                                               final Stack<String> jsonPath, final List<SchemaChange> changes) {
        jsonPath.push("additionalItems");
        if (original.permitsAdditionalItems() != update.permitsAdditionalItems()) {
            SchemaDiff.addChange(ADDITIONAL_ITEMS_CHANGED, jsonPath, changes);
        } else {
            SchemaDiff.recursiveCheck(original.getSchemaOfAdditionalItems(), update.getSchemaOfAdditionalItems(),
                    jsonPath, changes);
        }
        jsonPath.pop();
    }

    private static void compareItemSchemaArray(final ArraySchema original, final ArraySchema update,
                                               final Stack<String> jsonPath, final List<SchemaChange> changes) {
        final List<Schema> emptyList = ImmutableList.of();
        final List<Schema> originalSchemas = MoreObjects.firstNonNull(original.getItemSchemas(), emptyList);
        final List<Schema> updateSchemas = MoreObjects.firstNonNull(update.getItemSchemas(), emptyList);

        if (originalSchemas.size() != updateSchemas.size()) {
            SchemaDiff.addChange(NUMBER_OF_ITEMS_CHANGED, jsonPath, changes);
        } else {
            final Iterator<Schema> originalIterator = originalSchemas.iterator();
            final Iterator<Schema> updateIterator = updateSchemas.iterator();
            int index = 0;
            while (originalIterator.hasNext()) {
                jsonPath.push("items/" + index);
                SchemaDiff.recursiveCheck(originalIterator.next(), updateIterator.next(), jsonPath, changes);
                jsonPath.pop();
                index += 1;
            }
        }
    }

    private static void compareItemSchemaObject(final ArraySchema original, final ArraySchema update,
                                                final Stack<String> jsonPath, final List<SchemaChange> changes) {
        jsonPath.push("items");
        SchemaDiff.recursiveCheck(original.getAllItemSchema(), update.getAllItemSchema(), jsonPath, changes);
        jsonPath.pop();
    }
}
