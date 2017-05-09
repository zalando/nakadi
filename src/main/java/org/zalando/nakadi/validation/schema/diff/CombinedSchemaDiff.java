package org.zalando.nakadi.validation.schema.diff;

import org.everit.json.schema.CombinedSchema;
import org.everit.json.schema.Schema;
import org.zalando.nakadi.domain.SchemaChange;

import java.util.Iterator;
import java.util.List;
import java.util.Stack;

import static org.everit.json.schema.CombinedSchema.ALL_CRITERION;
import static org.everit.json.schema.CombinedSchema.ANY_CRITERION;
import static org.zalando.nakadi.domain.SchemaChange.Type.COMPOSITION_METHOD_CHANGED;
import static org.zalando.nakadi.domain.SchemaChange.Type.SUB_SCHEMA_CHANGED;

class CombinedSchemaDiff {
    static void recursiveCheck(final CombinedSchema combinedSchemaOriginal, final CombinedSchema combinedSchemaUpdate,
                                       final Stack<String> jsonPath,
                                       final List<SchemaChange> changes) {
        if(combinedSchemaOriginal.getSubschemas().size() != combinedSchemaUpdate.getSubschemas().size()) {
            SchemaDiff.addChange(SUB_SCHEMA_CHANGED, jsonPath, changes);
        } else {
            if (!combinedSchemaOriginal.getCriterion().equals(combinedSchemaUpdate.getCriterion())) {
                SchemaDiff.addChange(COMPOSITION_METHOD_CHANGED, jsonPath, changes);
            } else {
                final Iterator<Schema> originalIterator = combinedSchemaOriginal.getSubschemas().iterator();
                final Iterator<Schema> updateIterator = combinedSchemaUpdate.getSubschemas().iterator();
                int index = 0;
                while (originalIterator.hasNext()) {
                    jsonPath.push(validationCriteria(combinedSchemaOriginal.getCriterion()) + "/" + index);
                    SchemaDiff.recursiveCheck(originalIterator.next(), updateIterator.next(), jsonPath, changes);
                    jsonPath.pop();
                    index += 1;
                }
            }
        }
    }

    private static String validationCriteria(final CombinedSchema.ValidationCriterion criterion) {
        if (criterion.equals(ALL_CRITERION)) {
            return "allOf";
        } else if (criterion.equals(ANY_CRITERION)) {
            return "anyOf";
        } else {
            return "oneOf";
        }
    }
}
