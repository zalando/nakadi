package org.zalando.nakadi.validation.schema.diff;

import org.everit.json.schema.ArraySchema;
import org.everit.json.schema.CombinedSchema;
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

public class SchemaDiff {
    public List<SchemaChange> collectChanges(final Schema original, final Schema update) {
        final SchemaDiffState state = new SchemaDiffState();

        recursiveCheck(original, update, state);

        return state.getChanges();
    }

    static void recursiveCheck(
            final Schema original,
            final Schema update,
            final SchemaDiffState state) {

        if (original == null && update == null) {
            return;
        }

        if (update == null) {
            state.addChange(SCHEMA_REMOVED);
            return;
        }

        if (!original.getClass().equals(update.getClass())) {
            state.addChange(TYPE_CHANGED);
            return;
        }

        state.analyzeSchema(original, () -> {
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


}
