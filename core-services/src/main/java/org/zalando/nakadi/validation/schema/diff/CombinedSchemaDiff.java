package org.zalando.nakadi.validation.schema.diff;

import org.everit.json.schema.CombinedSchema;
import org.everit.json.schema.Schema;

import java.util.Iterator;

import static org.zalando.nakadi.domain.SchemaChange.Type.COMPOSITION_METHOD_CHANGED;
import static org.zalando.nakadi.domain.SchemaChange.Type.SUB_SCHEMA_CHANGED;

class CombinedSchemaDiff {
    static void recursiveCheck(final CombinedSchema combinedSchemaOriginal, final CombinedSchema combinedSchemaUpdate,
                               final SchemaDiffState state) {
        if (combinedSchemaOriginal.getSubschemas().size() != combinedSchemaUpdate.getSubschemas().size()) {
            state.addChange(SUB_SCHEMA_CHANGED);
        } else {
            if (!combinedSchemaOriginal.getCriterion().equals(combinedSchemaUpdate.getCriterion())) {
                state.addChange(COMPOSITION_METHOD_CHANGED);
            } else {
                final Iterator<Schema> originalIterator = combinedSchemaOriginal.getSubschemas().iterator();
                final Iterator<Schema> updateIterator = combinedSchemaUpdate.getSubschemas().iterator();
                int index = 0;
                while (originalIterator.hasNext()) {
                    state.runOnPath(validationCriteria(combinedSchemaOriginal.getCriterion()) + "/" + index, () -> {
                        SchemaDiff.recursiveCheck(originalIterator.next(), updateIterator.next(), state);
                    });
                    index += 1;
                }
            }
        }
    }

    private static String validationCriteria(final CombinedSchema.ValidationCriterion criterion) {
        if (criterion.equals(CombinedSchema.ALL_CRITERION)) {
            return "allOf";
        } else if (criterion.equals(CombinedSchema.ANY_CRITERION)) {
            return "anyOf";
        } else {
            return "oneOf";
        }
    }
}
