package org.zalando.nakadi.validation.schema.diff;

import org.everit.json.schema.Schema;
import org.zalando.nakadi.domain.SchemaChange;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

public class SchemaDiffState {
    private final Stack<String> jsonPath;
    private final List<SchemaChange> changes;
    private final List<Schema> schemasInAnalysis;

    public SchemaDiffState() {
        jsonPath = new Stack<>();
        changes = new ArrayList<>();
        schemasInAnalysis = new ArrayList<>();
    }

    public void analyzeSchema(final Schema schema, final Runnable r) {
        // The problem here that hashCode will die for recursive schema
        if (schemasInAnalysis.stream().anyMatch(s -> s == schema)) {
            return;
        }
        schemasInAnalysis.add(schema);
        try {
            r.run();
        } finally {
            schemasInAnalysis.remove(schemasInAnalysis.size() - 1);
        }
    }

    public void runOnPath(final String name, final Runnable r) {
        jsonPath.push(name);
        try {
            r.run();
        } finally {
            jsonPath.pop();
        }
    }

    public List<SchemaChange> getChanges() {
        return changes;
    }

    public void addChange(final SchemaChange.Type type) {
        changes.add(new SchemaChange(type, jsonPathString(jsonPath)));
    }

    public void addChange(final String attribute, final SchemaChange.Type type) {
        jsonPath.push(attribute);
        addChange(type);
        jsonPath.pop();
    }

    private static String jsonPathString(final List<String> jsonPath) {
        return "#/" + String.join("/", jsonPath);
    }
}
