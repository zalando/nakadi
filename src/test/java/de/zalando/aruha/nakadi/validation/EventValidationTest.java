package de.zalando.aruha.nakadi.validation;

import de.zalando.aruha.nakadi.domain.EventCategory;
import de.zalando.aruha.nakadi.domain.EventType;
import org.junit.Test;

import static de.zalando.aruha.nakadi.utils.TestUtils.buildEventType;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static uk.co.datumedge.hamcrest.json.SameJSONAs.sameJSONAs;

public class EventValidationTest {

    @Test
    public void whenTypeIsUndefinedThenReturnsPlainSchema() {
        final String schema = "{\"type\": \"object\", \"properties\": {\"foo\": {\"type\": \"string\"} }, \"required\": [\"foo\"]}";
        final EventType et = buildEventType("some-event-type", schema);

        assertThat(EventValidation.effectiveSchema(et).toString(), is(sameJSONAs(schema).allowingAnyArrayOrdering()));
    }

    @Test
    public void whenSchemaIsEmptyAddsPropetiesWithMetadata() {
        final String schema = "{}";
        final EventType et = buildEventType("some-event-type", schema);
        et.setCategory(EventCategory.BUSINESS);

        final String businessSchema = "{\"type\": \"object\", \"properties\": {\"metadata\": {\"type\": \"object\"} }, \"required\": [\"metadata\"]}";

        assertThat(EventValidation.effectiveSchema(et).toString(), is(sameJSONAs(businessSchema).allowingAnyArrayOrdering()));
    }

    @Test
    public void whenTypeIsBusinessThenAddMetadataField() {
        final String schema = "{\"type\": \"object\", \"properties\": {\"foo\": {\"type\": \"string\"} }, \"required\": [\"foo\"]}";
        final EventType et = buildEventType("some-event-type", schema);
        et.setCategory(EventCategory.BUSINESS);

        final String businessSchema = "{\"type\": \"object\", \"properties\": {\"foo\": {\"type\": \"string\"}, \"metadata\": {\"type\": \"object\"} }, \"required\": [\"foo\", \"metadata\"]}";

        assertThat(EventValidation.effectiveSchema(et).toString(), is(sameJSONAs(businessSchema).allowingAnyArrayOrdering()));
    }

    @Test
    public void addMetadataToRequiredArray() {
        final String schema = "{\"type\": \"object\", \"properties\": {\"foo\": {\"type\": \"string\"}, \"metadata\": {\"type\": \"object\"} }, \"required\": [\"foo\"]}";
        final EventType et = buildEventType("some-event-type", schema);
        et.setCategory(EventCategory.BUSINESS);

        final String businessSchema = "{\"type\": \"object\", \"properties\": {\"foo\": {\"type\": \"string\"}, \"metadata\": {\"type\": \"object\"} }, \"required\": [\"foo\", \"metadata\"]}";

        assertThat(EventValidation.effectiveSchema(et).toString(), is(sameJSONAs(businessSchema).allowingAnyArrayOrdering()));
    }

    @Test
    public void overridesCurrentDefinitionOfMetadata() {
        final String schema = "{\"type\": \"object\", \"properties\": {\"foo\": {\"type\": \"string\"}, \"metadata\": {\"type\": \"string\"} }, \"required\": [\"foo\", \"metadata\"]}";
        final EventType et = buildEventType("some-event-type", schema);
        et.setCategory(EventCategory.BUSINESS);

        final String businessSchema = "{\"type\": \"object\", \"properties\": {\"foo\": {\"type\": \"string\"}, \"metadata\": {\"type\": \"object\"} }, \"required\": [\"foo\", \"metadata\"]}";

        assertThat(EventValidation.effectiveSchema(et).toString(), is(sameJSONAs(businessSchema).allowingAnyArrayOrdering()));
    }

    @Test
    public void whenTypeIsDataChangeThenWrap() {
        final String schema = "{\"type\": \"object\", \"properties\": {\"foo\": {\"type\": \"string\"} }, \"required\": [\"foo\"]}";
        final EventType et = buildEventType("some-event-type", schema);
        et.setCategory(EventCategory.DATA);

        final String dataSchema = "{\"type\": \"object\", \"properties\": { \"data_type\": { \"type\": \"string\" }, \"data_op\": { \"type\": \"string\", \"enum\": [\"C\", \"U\", \"D\", \"S\"] }, \"metadata\": {\"type\": \"object\"}, \"data\": {\"type\": \"object\", \"properties\": {\"foo\": {\"type\": \"string\"} }, \"required\": [\"foo\"]} }, \"required\": [\"data\", \"data_op\", \"data_type\", \"metadata\"], \"additionalProperties\": false}";

        assertThat(EventValidation.effectiveSchema(et).toString(), is(sameJSONAs(dataSchema).allowingAnyArrayOrdering()));
    }
}
