package org.zalando.nakadi.validation;

import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.utils.EventTypeTestBuilder;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.zalando.nakadi.utils.TestUtils.readFile;
import static uk.co.datumedge.hamcrest.json.SameJSONAs.sameJSONObjectAs;

public class JsonSchemaEnrichmentTest {
    private final JsonSchemaEnrichment loader = new JsonSchemaEnrichment();

    @Test
    public void enforceStrict() throws Exception {
        final JSONArray testCases = new JSONArray(
                readFile("strict-validation.json"));

        for (final Object testCaseObject : testCases) {
            final JSONObject testCase = (JSONObject) testCaseObject;
            final String description = testCase.getString("description");
            final JSONObject original = testCase.getJSONObject("original_schema");
            final JSONObject effective = testCase.getJSONObject("effective_schema");

            final EventType eventType = EventTypeTestBuilder.builder().schema(original).build();

            assertThat(description, loader.effectiveSchema(eventType), is(sameJSONObjectAs(effective)));
        }
    }
}