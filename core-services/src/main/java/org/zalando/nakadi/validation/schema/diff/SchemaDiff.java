package org.zalando.nakadi.validation.schema.diff;

import org.everit.json.schema.ArraySchema;
import org.everit.json.schema.CombinedSchema;
import org.everit.json.schema.EmptySchema;
import org.everit.json.schema.EnumSchema;
import org.everit.json.schema.NumberSchema;
import org.everit.json.schema.ObjectSchema;
import org.everit.json.schema.ReferenceSchema;
import org.everit.json.schema.Schema;
import org.everit.json.schema.StringSchema;
import org.zalando.nakadi.domain.SchemaChange;

import java.util.List;
import java.util.Objects;

import static org.zalando.nakadi.domain.SchemaChange.Type.DESCRIPTION_CHANGED;
import static org.zalando.nakadi.domain.SchemaChange.Type.ID_CHANGED;
import static org.zalando.nakadi.domain.SchemaChange.Type.SCHEMA_REMOVED;
import static org.zalando.nakadi.domain.SchemaChange.Type.TITLE_CHANGED;
import static org.zalando.nakadi.domain.SchemaChange.Type.TYPE_CHANGED;
import static org.zalando.nakadi.domain.SchemaChange.Type.TYPE_NARROWED;

public class SchemaDiff {
    public List<SchemaChange> collectChanges(final Schema original, final Schema update) {
        final SchemaDiffState state = new SchemaDiffState();

        recursiveCheck(original, update, state);

        return state.getChanges();
    }

    static void recursiveCheck(
            final Schema originalIn,
            final Schema updateIn,
            final SchemaDiffState state) {

        if (originalIn == null && updateIn == null) {
            return;
        }

        if (updateIn == null) {
            state.addChange(SCHEMA_REMOVED);
            return;
        }

        if (originalIn == null) {
            state.addChange(SCHEMA_REMOVED);
            return;
        }

        final Schema original;
        final Schema update;
        if (!originalIn.getClass().equals(updateIn.getClass())) {
            // Tricky part. EmptySchema is the same as an empty ObjectSchema.
            if (originalIn instanceof EmptySchema && updateIn instanceof ObjectSchema) {
                original = replaceWithEmptyObjectSchema(originalIn);
                update = updateIn;
            } else if (typeNarrowed(originalIn, updateIn)) {
                state.addChange(TYPE_NARROWED);
                return;
            } else {
                state.addChange(TYPE_CHANGED);
                return;
            }
        } else {
            original = originalIn;
            update = updateIn;
        }

        state.analyzeSchema(originalIn, () -> {
            if (!Objects.equals(original.getId(), update.getId())) {
                state.addChange(ID_CHANGED);
            }

            if (!Objects.equals(original.getTitle(), update.getTitle())) {
                state.addChange(TITLE_CHANGED);
            }

            if (!Objects.equals(original.getDescription(), update.getDescription())) {
                state.addChange(DESCRIPTION_CHANGED);
            }

            if (original instanceof StringSchema) {
                StringSchemaDiff.recursiveCheck((StringSchema) original, (StringSchema) update, state);
            } else if (original instanceof NumberSchema) {
                NumberSchemaDiff.recursiveCheck((NumberSchema) original, (NumberSchema) update, state);
            } else if (original instanceof EnumSchema) {
                EnumSchemaDiff.recursiveCheck((EnumSchema) original, (EnumSchema) update, state);
            } else if (original instanceof CombinedSchema) {
                CombinedSchemaDiff.recursiveCheck((CombinedSchema) original, (CombinedSchema) update, state);
            } else if (original instanceof ObjectSchema) {
                ObjectSchemaDiff.recursiveCheck((ObjectSchema) original, (ObjectSchema) update, state);
            } else if (original instanceof ArraySchema) {
                ArraySchemaDiff.recursiveCheck((ArraySchema) original, (ArraySchema) update, state);
            } else if (original instanceof ReferenceSchema) {
                ReferenceSchemaDiff.recursiveCheck((ReferenceSchema) original, (ReferenceSchema) update, state);
            }
        });
    }

    private static boolean typeNarrowed(final Schema originalIn, final Schema updateIn) {
        return originalIn instanceof EmptySchema && !(updateIn instanceof EmptySchema);
    }

    private static Schema replaceWithEmptyObjectSchema(final Schema in) {
        return ObjectSchema.builder()
                .id(in.getId())
                .title(in.getTitle())
                .description(in.getDescription())
                .build();
    }


}
