package org.zalando.nakadi.validation.schema;

import org.everit.json.schema.Schema;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.zalando.nakadi.domain.SchemaChange;
import org.zalando.nakadi.validation.schema.diff.SchemaDiff;

import java.io.IOException;
import java.util.List;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.zalando.nakadi.utils.TestUtils.readFile;

public class SchemaDiffTest {
    private SchemaDiff service;

    @Before
    public void setUp() {
        this.service = new SchemaDiff();
    }

    @Test
    public void checkJsonSchemaCompatibility() throws Exception {
        final JSONArray testCases = new JSONArray(
                readFile("invalid-schema-evolution-examples.json"));

        for (final Object testCaseObject : testCases) {
            final JSONObject testCase = (JSONObject) testCaseObject;
            final Schema original = SchemaLoader.load(testCase.getJSONObject("original_schema"));
            final Schema update = SchemaLoader.load(testCase.getJSONObject("update_schema"));
            final List<String> errorMessages = testCase
                    .getJSONArray("errors")
                    .toList()
                    .stream()
                    .map(Object::toString)
                    .collect(toList());
            final String description = testCase.getString("description");

            assertThat(description, service.collectChanges(original, update).stream()
                    .map(change -> change.getType().toString() + " " + change.getJsonPath())
                    .collect(toList()), is(errorMessages));

        }
    }

    @Test
    public void testRecursiveCheck() throws IOException {
        final Schema original = SchemaLoader.load(new JSONObject(
                readFile("recursive-schema.json")));
        final Schema newOne = SchemaLoader.load(new JSONObject(
                readFile("recursive-schema.json")));
        assertTrue(service.collectChanges(original, newOne).isEmpty());
    }

    @Test
    public void testSchemaAddsProperties() {
        final Schema first = SchemaLoader.load(new JSONObject("{}"));

        final Schema second = SchemaLoader.load(new JSONObject("{\"properties\": {}}"));
        final List<SchemaChange> changes = service.collectChanges(first, second);
        assertTrue(changes.isEmpty());
    }
}