package org.zalando.nakadi.validation.schema;

import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.zalando.nakadi.utils.TestUtils.readFile;
import static uk.co.datumedge.hamcrest.json.SameJSONAs.sameJSONObjectAs;

public class SchemaGenerationTest {
    private SchemaGeneration service = new SchemaGeneration();

    @Test
    public void schemaFor() throws Exception {
        final JSONArray testCases = new JSONArray(readFile("org/zalando/nakadi/validation/schema-generation.json"));

        for(final Object testCaseObject : testCases) {
            final JSONObject testCase = (JSONObject) testCaseObject;
            final JSONObject event = testCase.getJSONObject("event");
            final JSONObject schema = testCase.getJSONObject("schema");
            final String description = testCase.getString("description");

            assertThat(description, service.schemaFor(event), is(sameJSONObjectAs(schema)));
        }
    }

}