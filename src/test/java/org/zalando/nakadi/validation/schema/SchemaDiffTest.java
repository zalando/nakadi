package org.zalando.nakadi.validation.schema;

import org.everit.json.schema.Schema;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.zalando.nakadi.validation.schema.diff.SchemaDiff;

import java.io.IOException;
import java.util.List;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.zalando.nakadi.utils.TestUtils.readFile;

public class SchemaDiffTest {
    private SchemaDiff service;

    @Before
    public void setUp() throws IOException {
        this.service = new SchemaDiff();
    }

    @Test
    public void checkJsonSchemaCompatibility() throws Exception {
        final JSONArray testCases = new JSONArray(
                readFile("org/zalando/nakadi/validation/invalid-schema-evolution-examples.json"));

        for(final Object testCaseObject : testCases) {
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
}