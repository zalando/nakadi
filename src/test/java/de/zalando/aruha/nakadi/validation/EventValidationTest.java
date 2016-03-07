package de.zalando.aruha.nakadi.validation;

import de.zalando.aruha.nakadi.domain.EventType;
import org.json.JSONObject;
import org.junit.Test;

import static de.zalando.aruha.nakadi.utils.TestUtils.buildEventType;
import static junit.framework.Assert.fail;
import static org.hamcrest.MatcherAssert.assertThat;
import static uk.co.datumedge.hamcrest.json.SameJSONAs.sameJSONAs;

public class EventValidationTest {
    @Test
    public void whenTypeIsUnknownThenReturnsPlainSchema() {
        final String schema = "{\"type\": \"object\", \"properties\": {\"foo\": {\"type\": \"string\"}, \"bar\": {\"type\": \"object\", \"properties\": {\"foo\": {\"type\": \"string\"}, \"bar\": {\"type\": \"string\"}}, \"required\": [\"foo\", \"bar\"]}}, \"required\": [\"foo\", \"bar\"]}";
        final EventType et = buildEventType("some-event-type", schema);


        assertThat(EventValidation.effectiveSchema(et).toString(), org.hamcrest.Matchers.is(sameJSONAs(schema)));
    }

    @Test
    public void whenTypeIsBusinessThenAddMetadataField() {
        fail();
    }

    @Test
    public void whenTypeIsDataChangeThenAddMetadataField() {
        fail();
    }

    @Test
    public void whenTypeIsDataChangeThenAddDataOpField() {
        fail();
    }

    @Test
    public void whenTypeIsDataChangeThenNestSchemaInData() {
        fail();
    }
}
