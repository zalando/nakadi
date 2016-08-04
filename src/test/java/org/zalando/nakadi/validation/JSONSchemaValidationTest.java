package org.zalando.nakadi.validation;

import org.json.JSONObject;
import org.junit.Test;
import org.zalando.nakadi.domain.EventCategory;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.ValidationStrategyConfiguration;
import org.zalando.nakadi.utils.EventTypeTestBuilder;

import java.util.Arrays;
import java.util.Optional;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.zalando.nakadi.utils.IsOptional.isAbsent;
import static org.zalando.nakadi.utils.TestUtils.readFile;

public class JSONSchemaValidationTest {

    static {
        ValidationStrategy.register(EventBodyMustRespectSchema.NAME, new EventBodyMustRespectSchema());
        ValidationStrategy.register(EventMetadataValidationStrategy.NAME, new EventMetadataValidationStrategy());
        ValidationStrategy.register(FieldNameMustBeSet.NAME, new FieldNameMustBeSet());
    }

    @Test
    public void schemaValidationShouldRespectEventTypeDefinition() {
        final EventType et = EventTypeTestBuilder.builder().name("some-event-type")
                .schema("{\"type\": \"object\", \"properties\": {\"foo\": {\"type\": \"string\"}, " +
                        "\"bar\": {\"type\": \"object\", \"properties\": {\"foo\": {\"type\": \"string\"}, " +
                        "\"bar\": {\"type\": \"string\"}}, \"required\": [\"foo\", \"bar\"]}}, \"required\":" +
                        " [\"foo\", \"bar\"]}").build();

        final ValidationStrategyConfiguration vsc1 = new ValidationStrategyConfiguration();
        vsc1.setStrategyName(EventBodyMustRespectSchema.NAME);
        et.getValidationStrategies().add(vsc1);

        final ValidationStrategyConfiguration vsc2 = new ValidationStrategyConfiguration();
        vsc2.setStrategyName(FieldNameMustBeSet.NAME);
        et.getValidationStrategies().add(vsc2);

        final EventTypeValidator validator = new EventTypeValidator(et).withConfiguration(vsc1).withConfiguration(vsc2);

        final JSONObject validEvent = new JSONObject(
                "{\"foo\": \"bar\", \"bar\": {\"foo\": \"baz\", \"bar\": \"baz\"}, " +
                        "\"extra\": \"i should be no problem\", \"name\": \"12345\"}");
        final JSONObject invalidEventMissingBar = new JSONObject(
                "{\"foo\": \"bar\", \"extra\": \"i should be no problem\", \"name\": \"12345\"}");
        final JSONObject invalidEventMissingNameField = new JSONObject(
                "{\"foo\": \"bar\", \"bar\": {\"foo\": \"baz\", \"bar\": \"baz\"}, " +
                        "\"extra\": \"i should be no problem\"}");
        final JSONObject nestedSchemaViolation = new JSONObject(
                "{\"bar\": {\"foobar\": \"baz\"}, \"extra\": \"i should be no problem\", \"name\": \"12345\"}");

        assertThat(validator.validate(validEvent), isAbsent());
        assertThat(validator.validate(invalidEventMissingBar).get().getMessage(),
                equalTo("#: required key [bar] not found"));
        assertThat(validator.validate(invalidEventMissingNameField).get().getMessage(), equalTo("name is required"));
        assertThat(validator.validate(nestedSchemaViolation).get().getMessage(),
                equalTo("#: 2 schema violations found\n#/bar: 2 schema violations found\n#/bar: required key [foo]" +
                        " not found\n#/bar: required key [bar] not found\n#: required key [foo] not found"));
    }

    @Test
    public void schemaValidationShouldRespectIgnoreConfigurationMatchRegular() {
        final EventType et = EventTypeTestBuilder.builder().name("some-event-type")
                .schema("{\"type\": \"object\", \"properties\": {\"field-that-will-not-be-found\": " +
                        "{\"type\": \"object\"}, \"event-type\": {\"type\": \"string\"}}, \"required\": " +
                        "[\"field-that-will-not-be-found\", \"event-type\"]}").build();

        final ValidationStrategyConfiguration vsc1 = new ValidationStrategyConfiguration();
        vsc1.setStrategyName(EventBodyMustRespectSchema.NAME);
        vsc1.setAdditionalConfiguration(new JSONObject(
                "{\"overrides\": [{\"qualifier\": {\"field\": \"event-type\", \"match\" : \"D\"}, " +
                        "\"ignoredProperties\": [\"field-that-will-not-be-found\"]}]}"));
        et.getValidationStrategies().add(vsc1);

        final EventTypeValidator validator = new EventTypeValidator(et).withConfiguration(vsc1);

        final JSONObject event = new JSONObject(
                "{\"event-type\" : \"X\", \"extra\": \"i should be no problem\", \"name\": \"12345\", " +
                        "\"field-that-will-not-be-found\": " +
                        "{\"val\": \"i must be present since the matcher will not succeed\"}}");
        final JSONObject eventDelete = new JSONObject(
                "{\"event-type\" : \"D\"}");
        final JSONObject invalidEvent = new JSONObject(
                "{\"event-type\" : \"X\", \"extra\": \"i should be no problem\", \"name\": \"12345\"}");
        assertThat(validator.validate(event), isAbsent());
        assertThat(validator.validate(eventDelete), isAbsent());
        assertThat(validator.validate(invalidEvent).get().getMessage(), equalTo("#: required key " +
                "[field-that-will-not-be-found] not found"));
    }

    @Test
    public void schemaValidationShouldRespectIgnoreConfigurationMatchQualified() {
        final EventType et = EventTypeTestBuilder.builder().name("some-event-type")
                .schema("{\"type\": \"object\", \"properties\": {\"field-that-will-not-be-found\": " +
                        "{\"type\": \"string\"}, \"event-type\": {\"type\": \"string\"}}, \"required\": " +
                        "[\"field-that-will-not-be-found\", \"event-type\"]}").build();

        final ValidationStrategyConfiguration vsc1 = new ValidationStrategyConfiguration();
        vsc1.setStrategyName(EventBodyMustRespectSchema.NAME);
        vsc1.setAdditionalConfiguration(new JSONObject(
                "{\"overrides\": [{\"qualifier\": {\"field\": \"event-type\", \"match\" : \"D\"}, " +
                        "\"ignoredProperties\": [\"field-that-will-not-be-found\"]}]}"));
        et.getValidationStrategies().add(vsc1);

        final EventTypeValidator validator = new EventTypeValidator(et).withConfiguration(vsc1);

        final JSONObject event = new JSONObject(
                "{\"event-type\" : \"X\", \"field-that-will-not-be-found\": \"some useless value\"," +
                        " \"extra\": \"i should be no problem\", \"name\": \"12345\"}");
        final JSONObject eventDelete = new JSONObject(
                "{\"event-type\" : \"D\", \"extra\": \"i should be no problem\"}");
        assertThat(validator.validate(event), isAbsent());
        assertThat(validator.validate(eventDelete), isAbsent());
    }

    @Test
    public void validationOfBusinessEventShouldRequiredMetadata() {
        final EventType et = EventTypeTestBuilder.builder().name("some-event-type")
                .schema(basicSchema()).build();
        et.setCategory(EventCategory.BUSINESS);

        final JSONObject event = new JSONObject("{ \"foo\": \"bar\" }");

        final Optional<ValidationError> error = EventValidation.forType(et).validate(event);

        assertThat(error.get().getMessage(), equalTo("#: required key [metadata] not found"));
    }

    @Test
    public void validationOfDataChangeEventRequiresExtraFields() {
        final EventType et = EventTypeTestBuilder.builder().name("some-event-type")
                .schema(basicSchema()).build();
        et.setCategory(EventCategory.DATA);

        final JSONObject event = new JSONObject("{ \"data\": { \"foo\": \"bar\" } }");

        final Optional<ValidationError> error = EventValidation.forType(et).validate(event);

        assertThat(error.get().getMessage(), equalTo("#: 3 schema violations found\n#: required key [metadata] " +
                "not found\n#: required key [data_op] not found\n#: required key [data_type] not found"));
    }

    @Test
    public void validationOfDataChangeEventShouldNotAllowAdditionalFieldsAtTheRootLevelObject() {
        final EventType et = EventTypeTestBuilder.builder().name("some-event-type").schema(basicSchema()).build();
        et.setCategory(EventCategory.DATA);

        final JSONObject event = dataChangeEvent();
        event.put("foo", "anything");

        final Optional<ValidationError> error = EventValidation.forType(et).validate(event);

        assertThat(error.get().getMessage(), equalTo("#: extraneous key [foo] is not permitted"));
    }

    @Test
    public void requireMetadataEventTypeToBeTheSameAsEventTypeName() {
        final EventType et = EventTypeTestBuilder.builder().name("some-event-type").schema(basicSchema()).build();
        et.setCategory(EventCategory.BUSINESS);

        final JSONObject event = businessEvent();
        event.getJSONObject("metadata").put("event_type", "different-from-event-name");

        final Optional<ValidationError> error = EventValidation.forType(et).validate(event);

        assertThat(error.get().getMessage(), equalTo("#/metadata/event_type: different-from-event-name " +
                "is not a valid enum value"));
    }

    @Test
    public void requireMetadataOccurredAt() {
        final EventType et = EventTypeTestBuilder.builder().name("some-event-type").schema(basicSchema()).build();
        et.setCategory(EventCategory.BUSINESS);

        final JSONObject event = businessEvent();
        event.getJSONObject("metadata").remove("occurred_at");

        final Optional<ValidationError> error = EventValidation.forType(et).validate(event);

        assertThat(error.get().getMessage(), equalTo("#/metadata: required key [occurred_at] not found"));
    }

    @Test
    public void requireEidToBeFormattedAsUUID() {
        final EventType et = EventTypeTestBuilder.builder().name("some-event-type").schema(basicSchema()).build();
        et.setCategory(EventCategory.BUSINESS);

        final JSONObject event = businessEvent();
        event.getJSONObject("metadata").put("eid", "x");

        final Optional<ValidationError> error = EventValidation.forType(et).validate(event);

        assertThat(error.get().getMessage(), equalTo("#/metadata/eid: string [x] does not match pattern " +
                "^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$"));
    }

    @Test
    public void acceptsDefinitionsOnDataChangeEvents() throws Exception {
        final JSONObject schema = new JSONObject(readFile("product-json-schema.json"));
        final EventType et = EventTypeTestBuilder.builder().name("some-event-type").schema(schema).build();
        et.setCategory(EventCategory.DATA);
        final JSONObject event = new JSONObject(readFile("product-event.json"));

        final Optional<ValidationError> error = EventValidation.forType(et).validate(event);

        assertThat(error, isAbsent());
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

    private JSONObject businessEvent() {
        final JSONObject event = new JSONObject();
        event.put("foo", "bar");
        event.put("metadata", metadata());

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
