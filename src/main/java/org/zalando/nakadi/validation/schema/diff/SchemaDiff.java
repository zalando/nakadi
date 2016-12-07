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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Stack;

import static org.zalando.nakadi.domain.SchemaChange.Type.DESCRIPTION_CHANGED;
import static org.zalando.nakadi.domain.SchemaChange.Type.ID_CHANGED;
import static org.zalando.nakadi.domain.SchemaChange.Type.SCHEMA_REMOVED;
import static org.zalando.nakadi.domain.SchemaChange.Type.TITLE_CHANGED;
import static org.zalando.nakadi.domain.SchemaChange.Type.TYPE_CHANGED;

public class SchemaDiff {
    public List<SchemaChange> collectChanges(final Schema original, final Schema update) {
        final List<SchemaChange> changes = new ArrayList<>();
        final Stack<String> jsonPath = new Stack<>();

        recursiveCheck(original, update, jsonPath, changes);

        return changes;
    }

    static void recursiveCheck(
            final Schema original,
            final Schema update,
            final Stack<String> jsonPath,
            final List<SchemaChange> changes) {

        if (original == null && update == null) {
            return;
        }

        if (update == null) {
            addChange(SCHEMA_REMOVED, jsonPath, changes);
            return;
        }

        if (!original.getClass().equals(update.getClass())) {
            addChange(TYPE_CHANGED, jsonPath, changes);
            return;
        }

        if (!Objects.equals(original.getId(), update.getId())) {
            addChange(ID_CHANGED, jsonPath, changes);
        }

        if (!Objects.equals(original.getTitle(), update.getTitle())) {
            addChange(TITLE_CHANGED, jsonPath, changes);
        }

        if (!Objects.equals(original.getDescription(), update.getDescription())) {
            addChange(DESCRIPTION_CHANGED, jsonPath, changes);
        }

        if (original instanceof StringSchema) {
            StringSchemaDiff.recursiveCheck((StringSchema) original, (StringSchema) update, jsonPath, changes);
        } else if (original instanceof NumberSchema) {
            NumberSchemaDiff.recursiveCheck((NumberSchema) original, (NumberSchema) update, jsonPath, changes);
        } else if (original instanceof EnumSchema) {
            EnumSchemaDiff.recursiveCheck((EnumSchema) original, (EnumSchema) update, jsonPath, changes);
        } else if (original instanceof CombinedSchema) {
            CombinedSchemaDiff.recursiveCheck((CombinedSchema) original, (CombinedSchema) update, jsonPath, changes);
        } else if (original instanceof ObjectSchema) {
            ObjectSchemaDiff.recursiveCheck((ObjectSchema) original, (ObjectSchema) update, jsonPath, changes);
        } else if (original instanceof ArraySchema) {
            ArraySchemaDiff.recursiveCheck((ArraySchema) original, (ArraySchema) update, jsonPath, changes);
        } else if (original instanceof ReferenceSchema) {
            ReferenceSchemaDiff.recursiveCheck((ReferenceSchema) original, (ReferenceSchema) update, jsonPath, changes);
        }
    }

    static void addChange(final SchemaChange.Type type, final Stack<String> jsonPath,
                          final List<SchemaChange> changes) {
        changes.add(new SchemaChange(type, jsonPathString(jsonPath)));
    }

    static void addChange(final String attribute, final SchemaChange.Type type, final Stack<String> jsonPath,
                          final List<SchemaChange> changes) {
        jsonPath.push(attribute);
        addChange(type, jsonPath, changes);
        jsonPath.pop();
    }

    private static String jsonPathString(final List<String> jsonPath) {
        return "#/" + String.join("/", jsonPath);
    }
}
