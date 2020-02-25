package org.zalando.nakadi.validation;

import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;
import org.zalando.nakadi.domain.CleanupPolicy;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.utils.EventTypeTestBuilder;

import java.util.UUID;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.zalando.nakadi.utils.TestUtils.readFile;
import static uk.co.datumedge.hamcrest.json.SameJSONAs.sameJSONObjectAs;

public class JsonSchemaEnrichmentTest {
    private final JsonSchemaEnrichment loader = new JsonSchemaEnrichment(null);

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

    @Test
    public void testMetadata() {
        final String randomEventTypeName = UUID.randomUUID().toString();
        for (final CleanupPolicy policy : CleanupPolicy.values()) {
            final JSONObject metadata = loader.createMetadata(randomEventTypeName, policy);
            Assert.assertNotNull(metadata);
            Assert.assertTrue(metadata.toString(0).contains(randomEventTypeName));
        }
    }
}