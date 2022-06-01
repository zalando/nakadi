package org.zalando.nakadi.validation;

import org.hamcrest.CoreMatchers;
import org.hamcrest.Matchers;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.core.io.DefaultResourceLoader;
import org.zalando.nakadi.domain.CompatibilityMode;
import org.zalando.nakadi.domain.EventCategory;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.utils.EventTypeTestBuilder;
import org.zalando.nakadi.utils.IsOptional;
import org.zalando.nakadi.utils.TestUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;


public class JSONSchemaValidationTest {

    private EventValidatorBuilder eventValidatorBuilder;

    @Before
    public void before() throws IOException {
        eventValidatorBuilder = new EventValidatorBuilder(
                new JsonSchemaEnrichment(new DefaultResourceLoader(), "classpath:schema_metadata.json"));
    }

    @Test
    public void validationOfBusinessEventShouldRequiredMetadata() {
        final JSONObject schema = basicSchema();
        final EventType et = EventTypeTestBuilder.builder().name("some-event-type")
                .schema(schema).build();
        et.setCategory(EventCategory.BUSINESS);

        final JSONObject event = new JSONObject("{ \"foo\": \"bar\" }");

        final Optional<ValidationError> error = eventValidatorBuilder.build(et).validate(event);

        Assert.assertThat(
                error.get().getMessage(),
                CoreMatchers.equalTo("#: required key [metadata] not found"));
    }

    @Test
    public void validationOfBusinessEventShouldAllowSpanCtxtInMetadata() {
        final JSONObject schema = basicSchema();
        final EventType et = EventTypeTestBuilder.builder().name("some-event-type")
                .schema(schema).compatibilityMode(CompatibilityMode.COMPATIBLE).build();
        et.setCategory(EventCategory.BUSINESS);

        final JSONObject validEvent = new JSONObject("{\"metadata\":{" +
                "\"occurred_at\":\"1992-08-03T10:00:00Z\"," +
                "\"eid\":\"329ed3d2-8366-11e8-adc0-fa7ae01bbebc\"," +
                "\"span_ctx\": {" +
                "      \"ot-tracer-spanid\": \"b268f901d5f2b865\"," +
                "      \"ot-tracer-traceid\": \"e9435c17dabe8238\"," +
                "      \"ot-baggage-foo\": \"bar\"" +
                "}}," +
                "\"foo\": \"bar\"}");

        final Optional<ValidationError> noError = eventValidatorBuilder.build(et).validate(validEvent);

        Assert.assertThat(noError, IsOptional.isAbsent());

        final JSONObject invalidEvent = new JSONObject("{\"metadata\":{" +
                "\"occurred_at\":\"1992-08-03T10:00:00Z\"," +
                "\"eid\":\"329ed3d2-8366-11e8-adc0-fa7ae01bbebc\"," +
                "\"span_ctx\": {" +
                "      \"ot-tracer-spanid\": 42," +
                "      \"ot-tracer-traceid\": \"e9435c17dabe8238\"," +
                "      \"ot-baggage-foo\": \"bar\"" +
                "}}," +
                "\"foo\": \"bar\"}");

        final Optional<ValidationError> error = eventValidatorBuilder.build(et).validate(invalidEvent);

        Assert.assertThat(error.get().getMessage(),
                CoreMatchers.equalTo("#/metadata/span_ctx/ot-tracer-spanid: expected type: String, found: Integer"));
    }

    @Test
    public void validationOfDataChangeEventRequiresExtraFields() {
        final JSONObject schema = basicSchema();
        final EventType et = EventTypeTestBuilder.builder().name("some-event-type")
                .schema(schema).build();
        et.setCategory(EventCategory.DATA);

        final JSONObject event = new JSONObject("{ \"data\": { \"foo\": \"bar\" } }");

        final Optional<ValidationError> error = eventValidatorBuilder.build(et).validate(event);

        Assert.assertThat(
                error.get().getMessage(),
                CoreMatchers.equalTo("#: 3 schema violations found\n#: required key [metadata] " +
                        "not found\n#: required key [data_op] not found\n#: required key [data_type] not found"));
    }

    @Test
    public void validationOfDataChangeEventShouldNotAllowAdditionalFieldsAtTheRootLevelObject() {
        final JSONObject schema = basicSchema();
        final EventType et = EventTypeTestBuilder.builder().name("some-event-type").schema(schema).build();
        et.setCategory(EventCategory.DATA);

        final JSONObject event = dataChangeEvent();
        event.put("foo", "anything");

        final Optional<ValidationError> error = eventValidatorBuilder.build(et).validate(event);

        Assert.assertThat(
                error.get().getMessage(),
                CoreMatchers.equalTo("#: extraneous key [foo] is not permitted"));
    }

    @Test
    public void requireMetadataEventTypeToBeTheSameAsEventTypeName() {
        final JSONObject schema = basicSchema();
        final EventType et = EventTypeTestBuilder.builder().name("some-event-type").schema(schema).build();
        et.setCategory(EventCategory.BUSINESS);

        final JSONObject event = businessEvent();
        event.getJSONObject("metadata").put("event_type", "different-from-event-name");

        final Optional<ValidationError> error = eventValidatorBuilder.build(et).validate(event);

        Assert.assertThat(
                error.get().getMessage(),
                CoreMatchers.equalTo(
                        "#/metadata/event_type: different-from-event-name is not a valid enum value"));
    }

    @Test
    public void requireMetadataOccurredAt() {
        final JSONObject schema = basicSchema();
        final EventType et = EventTypeTestBuilder.builder().name("some-event-type").schema(schema).build();
        et.setCategory(EventCategory.BUSINESS);

        final JSONObject event = businessEvent();
        event.getJSONObject("metadata").remove("occurred_at");

        final Optional<ValidationError> error = eventValidatorBuilder.build(et).validate(event);

        Assert.assertThat(
                error.get().getMessage(),
                CoreMatchers.equalTo("#/metadata: required key [occurred_at] not found"));
    }

    @Test
    public void requireEidToBeFormattedAsUUID() {
        final JSONObject schema = basicSchema();
        final EventType et = EventTypeTestBuilder.builder().name("some-event-type").schema(schema).build();
        et.setCategory(EventCategory.BUSINESS);

        final JSONObject event = businessEvent();
        event.getJSONObject("metadata").put("eid", "x");

        final Optional<ValidationError> error = eventValidatorBuilder.build(et).validate(event);

        Assert.assertThat(
                error.get().getMessage(),
                CoreMatchers.equalTo("#/metadata/eid: string [x] does not match pattern " +
                        "^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$"));
    }

    @Test
    public void requirePatternMatchingToBeFast() {
        final JSONObject schema = patternSchema();
        final EventType et = EventTypeTestBuilder.builder().name("some-event-type").schema(schema).build();
        et.setCategory(EventCategory.UNDEFINED);

        final long startTime = System.currentTimeMillis();

        final JSONObject event = undefinedEvent();
        final Optional<ValidationError> error = eventValidatorBuilder.build(et).validate(event);

        final long duration = System.currentTimeMillis() - startTime;

        Assert.assertThat(error, IsOptional.isAbsent());
        Assert.assertThat(duration, Matchers.lessThan(100L));
    }

    @Test
    public void acceptsDefinitionsOnDataChangeEvents() throws Exception {
        final JSONObject schema = new JSONObject(TestUtils.readFile("product-json-schema.json"));
        final EventType et = EventTypeTestBuilder.builder().name("some-event-type").schema(schema).build();
        et.setCategory(EventCategory.DATA);
        final JSONObject event = new JSONObject(TestUtils.readFile("product-event.json"));

        final Optional<ValidationError> error = eventValidatorBuilder.build(et).validate(event);

        Assert.assertThat(error, IsOptional.isAbsent());
    }

    private JSONObject basicSchema() {
        final JSONObject schema = new JSONObject();
        final JSONObject string = new JSONObject();
        string.put("type", "string");

        final JSONObject properties = new JSONObject();
        properties.put("foo", string);

        schema.put("type", "object");
        schema.put("required", Arrays.asList(new String[]{"foo"}));
        schema.put("properties", properties);

        return schema;
    }

    private JSONObject patternSchema() {
        final JSONObject schema = new JSONObject();
        final JSONObject string = new JSONObject();
        string.put("type", "string");
        string.put("pattern", "a?a?a?a?a?a?a?a?a?a?a?a?a?a?a?a?a?a?a?a?a?a?a?a?a?a?a?a?aaaaaaaaaaaaaaaaaaaaaaaaaaaa");

        final JSONObject properties = new JSONObject();
        properties.put("foo", string);

        schema.put("type", "object");
        schema.put("required", Arrays.asList(new String[]{"foo"}));
        schema.put("properties", properties);

        return schema;
    }

    private JSONObject businessEvent() {
        final JSONObject event = new JSONObject();
        event.put("foo", "bar");
        event.put("metadata", metadata());

        return event;
    }

    private JSONObject undefinedEvent() {
        final JSONObject event = new JSONObject();
        event.put("foo", "aaaaaaaaaaaaaaaaaaaaaaaaaaaa");

        return event;
    }

    private JSONObject dataChangeEvent() {
        final JSONObject event = new JSONObject();

        final JSONObject data = new JSONObject();
        data.put("foo", "bar");

        event.put("data", data);
        event.put("data_op", "C");
        event.put("data_type", "event-name");
        event.put("metadata", metadata());

        return event;
    }

    private JSONObject metadata() {
        final JSONObject metadata = new JSONObject();
        metadata.put("eid", "de305d54-75b4-431b-adb2-eb6b9e546014");
        metadata.put("occurred_at", "1996-12-19T16:39:57-08:00");

        return metadata;
    }
}
