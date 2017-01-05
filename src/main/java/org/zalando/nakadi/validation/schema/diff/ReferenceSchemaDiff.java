package org.zalando.nakadi.validation.schema.diff;

import org.everit.json.schema.ReferenceSchema;
import org.zalando.nakadi.domain.SchemaChange;

import java.util.List;
import java.util.Stack;

public class ReferenceSchemaDiff {
    static void recursiveCheck(final ReferenceSchema referenceSchemaOriginal,
                                       final ReferenceSchema referenceSchemaUpdate, final Stack<String> jsonPath,
                                       final List<SchemaChange> changes) {
        jsonPath.push("$ref");
        SchemaDiff.recursiveCheck(referenceSchemaOriginal.getReferredSchema(),
                referenceSchemaUpdate.getReferredSchema(), jsonPath, changes);
        jsonPath.pop();
    }
}
