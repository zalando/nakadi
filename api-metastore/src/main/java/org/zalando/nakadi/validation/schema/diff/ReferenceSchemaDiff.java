package org.zalando.nakadi.validation.schema.diff;

import org.everit.json.schema.ReferenceSchema;

public class ReferenceSchemaDiff {
    static void recursiveCheck(
            final ReferenceSchema referenceSchemaOriginal, final ReferenceSchema referenceSchemaUpdate,
            final SchemaDiffState state) {
        state.runOnPath("$ref", () -> {
            SchemaDiff.recursiveCheck(referenceSchemaOriginal.getReferredSchema(),
                    referenceSchemaUpdate.getReferredSchema(), state);
        });
    }
}
