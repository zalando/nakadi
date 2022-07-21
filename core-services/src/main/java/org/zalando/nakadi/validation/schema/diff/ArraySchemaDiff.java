package org.zalando.nakadi.validation.schema.diff;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import org.everit.json.schema.ArraySchema;
import org.everit.json.schema.Schema;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import static org.zalando.nakadi.domain.SchemaChange.Type.ADDITIONAL_ITEMS_CHANGED;
import static org.zalando.nakadi.domain.SchemaChange.Type.ATTRIBUTE_VALUE_CHANGED;
import static org.zalando.nakadi.domain.SchemaChange.Type.NUMBER_OF_ITEMS_CHANGED;

public class ArraySchemaDiff {
    static void recursiveCheck(final ArraySchema original, final ArraySchema update, final SchemaDiffState state) {
        compareItemSchemaObject(original, update, state);

        compareItemSchemaArray(original, update, state);

        compareAdditionalItems(original, update, state);

        compareAttributes(original, update, state);
    }

    private static void compareAttributes(
            final ArraySchema original, final ArraySchema update, final SchemaDiffState state) {
        if (!Objects.equals(original.getMaxItems(), update.getMaxItems())) {
            state.addChange("maxItems", ATTRIBUTE_VALUE_CHANGED);
        }

        if (!Objects.equals(original.getMinItems(), update.getMinItems())) {
            state.addChange("minItems", ATTRIBUTE_VALUE_CHANGED);
        }

        if (original.needsUniqueItems() != update.needsUniqueItems()) {
            state.addChange("uniqueItems", ATTRIBUTE_VALUE_CHANGED);
        }
    }

    private static void compareAdditionalItems(
            final ArraySchema original, final ArraySchema update, final SchemaDiffState state) {
        state.runOnPath("additionalItems", () -> {
            if (original.permitsAdditionalItems() != update.permitsAdditionalItems()) {
                state.addChange(ADDITIONAL_ITEMS_CHANGED);
            } else {
                SchemaDiff.recursiveCheck(original.getSchemaOfAdditionalItems(), update.getSchemaOfAdditionalItems(),
                        state);
            }
        });
    }

    private static void compareItemSchemaArray(
            final ArraySchema original, final ArraySchema update, final SchemaDiffState state) {
        final List<Schema> emptyList = ImmutableList.of();
        final List<Schema> originalSchemas = MoreObjects.firstNonNull(original.getItemSchemas(), emptyList);
        final List<Schema> updateSchemas = MoreObjects.firstNonNull(update.getItemSchemas(), emptyList);

        if (originalSchemas.size() != updateSchemas.size()) {
            state.addChange(NUMBER_OF_ITEMS_CHANGED);
        } else {
            final Iterator<Schema> originalIterator = originalSchemas.iterator();
            final Iterator<Schema> updateIterator = updateSchemas.iterator();
            int index = 0;
            while (originalIterator.hasNext()) {
                state.runOnPath("items/" + index, () -> {
                    SchemaDiff.recursiveCheck(originalIterator.next(), updateIterator.next(), state);
                });
                index += 1;
            }
        }
    }

    private static void compareItemSchemaObject(
            final ArraySchema original, final ArraySchema update, final SchemaDiffState state) {
        state.runOnPath("items", () -> {
            SchemaDiff.recursiveCheck(original.getAllItemSchema(), update.getAllItemSchema(), state);
        });
    }
}
