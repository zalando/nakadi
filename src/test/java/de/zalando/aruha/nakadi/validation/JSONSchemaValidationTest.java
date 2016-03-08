package de.zalando.aruha.nakadi.validation;

import de.zalando.aruha.nakadi.domain.EventCategory;
import de.zalando.aruha.nakadi.domain.EventType;
import de.zalando.aruha.nakadi.domain.ValidationStrategyConfiguration;
import org.json.JSONObject;
import org.junit.Test;

import java.util.Optional;

import static de.zalando.aruha.nakadi.utils.IsOptional.isAbsent;
import static de.zalando.aruha.nakadi.utils.TestUtils.buildEventType;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public class JSONSchemaValidationTest {

    // FIXME: once we are fully wired up and other validations exist, this should be removed from here and the
    // registration should be done in a common place.
    static {
        ValidationStrategy.register(EventBodyMustRespectSchema.NAME, new EventBodyMustRespectSchema());
        ValidationStrategy.register(FieldNameMustBeSet.NAME, new FieldNameMustBeSet());
    }

    @Test
    public void schemaValidationShouldRespectEventTypeDefinition() {
        final EventType et = buildEventType("some-event-type",
                    "{\"type\": \"object\", \"properties\": {\"foo\": {\"type\": \"string\"}, \"bar\": {\"type\": \"object\", \"properties\": {\"foo\": {\"type\": \"string\"}, \"bar\": {\"type\": \"string\"}}, \"required\": [\"foo\", \"bar\"]}}, \"required\": [\"foo\", \"bar\"]}");

        final ValidationStrategyConfiguration vsc1 = new ValidationStrategyConfiguration();
        vsc1.setStrategyName(EventBodyMustRespectSchema.NAME);
        et.getValidationStrategies().add(vsc1);

        final ValidationStrategyConfiguration vsc2 = new ValidationStrategyConfiguration();
        vsc2.setStrategyName(FieldNameMustBeSet.NAME);
        et.getValidationStrategies().add(vsc2);

        final EventTypeValidator validator = new EventTypeValidator(et).withConfiguration(vsc1).withConfiguration(vsc2);

        final JSONObject validEvent = new JSONObject(
                "{\"foo\": \"bar\", \"bar\": {\"foo\": \"baz\", \"bar\": \"baz\"}, \"extra\": \"i should be no problem\", \"name\": \"12345\"}");
        final JSONObject invalidEventMissingBar = new JSONObject(
                "{\"foo\": \"bar\", \"extra\": \"i should be no problem\", \"name\": \"12345\"}");
        final JSONObject invalidEventMissingNameField = new JSONObject(
                "{\"foo\": \"bar\", \"bar\": {\"foo\": \"baz\", \"bar\": \"baz\"}, \"extra\": \"i should be no problem\"}");
        final JSONObject nestedSchemaViolation = new JSONObject(
                "{\"bar\": {\"foobar\": \"baz\"}, \"extra\": \"i should be no problem\", \"name\": \"12345\"}");

        assertThat(validator.validate(validEvent), isAbsent());
        assertThat(validator.validate(invalidEventMissingBar).get().getMessage(), equalTo("#: required key [bar] not found"));
        assertThat(validator.validate(invalidEventMissingNameField).get().getMessage(), equalTo("name is required"));
        assertThat(validator.validate(nestedSchemaViolation).get().getMessage(),
                equalTo("#: 2 schema violations found\n#/bar: 2 schema violations found\n#/bar: required key [foo] not found\n#/bar: required key [bar] not found\n#: required key [foo] not found"));
    }

    @Test
    public void schemaValidationShouldRespectIgnoreConfigurationMatchRegular() {
        final EventType et = buildEventType("some-event-type",
                    "{\"type\": \"object\", \"properties\": {\"field-that-will-not-be-found\": {\"type\": \"object\"}, \"event-type\": {\"type\": \"string\"}}, \"required\": [\"field-that-will-not-be-found\", \"event-type\"]}");

        final ValidationStrategyConfiguration vsc1 = new ValidationStrategyConfiguration();
        vsc1.setStrategyName(EventBodyMustRespectSchema.NAME);
        vsc1.setAdditionalConfiguration(new JSONObject(
                "{\"overrides\": [{\"qualifier\": {\"field\": \"event-type\", \"match\" : \"D\"}, \"ignoredProperties\": [\"field-that-will-not-be-found\"]}]}"));
        et.getValidationStrategies().add(vsc1);

        final EventTypeValidator validator = new EventTypeValidator(et).withConfiguration(vsc1);

        final JSONObject event = new JSONObject(
                "{\"event-type\" : \"X\", \"extra\": \"i should be no problem\", \"name\": \"12345\", \"field-that-will-not-be-found\": {\"val\": \"i must be present since the matcher will not succeed\"}}");
        final JSONObject eventDelete = new JSONObject(
                "{\"event-type\" : \"D\"}");
        final JSONObject invalidEvent = new JSONObject(
                "{\"event-type\" : \"X\", \"extra\": \"i should be no problem\", \"name\": \"12345\"}");
        assertThat(validator.validate(event), isAbsent());
        assertThat(validator.validate(eventDelete), isAbsent());
        assertThat(validator.validate(invalidEvent).get().getMessage(), equalTo("#: required key [field-that-will-not-be-found] not found"));
    }

    @Test
    public void schemaValidationShouldRespectIgnoreConfigurationMatchQualified() {
        final EventType et = buildEventType("some-event-type",
                    "{\"type\": \"object\", \"properties\": {\"field-that-will-not-be-found\": {\"type\": \"string\"}, \"event-type\": {\"type\": \"string\"}}, \"required\": [\"field-that-will-not-be-found\", \"event-type\"]}");

        final ValidationStrategyConfiguration vsc1 = new ValidationStrategyConfiguration();
        vsc1.setStrategyName(EventBodyMustRespectSchema.NAME);
        vsc1.setAdditionalConfiguration(new JSONObject(
                "{\"overrides\": [{\"qualifier\": {\"field\": \"event-type\", \"match\" : \"D\"}, \"ignoredProperties\": [\"field-that-will-not-be-found\"]}]}"));
        et.getValidationStrategies().add(vsc1);

        final EventTypeValidator validator = new EventTypeValidator(et).withConfiguration(vsc1);

        final JSONObject event = new JSONObject(
                "{\"event-type\" : \"X\", \"field-that-will-not-be-found\": \"some useless value\", \"extra\": \"i should be no problem\", \"name\": \"12345\"}");
        final JSONObject eventDelete = new JSONObject(
                "{\"event-type\" : \"D\", \"extra\": \"i should be no problem\"}");
        assertThat(validator.validate(event), isAbsent());
        assertThat(validator.validate(eventDelete), isAbsent());
    }

    @Test
    public void validationOfBusinessEventShouldRequiredMetadata() {
        final EventType et = buildEventType("some-event-type",
                "{\"type\": \"object\", \"properties\": {\"foo\": {\"type\": \"string\"} }, \"required\": [\"foo\"]}");
        et.setCategory(EventCategory.BUSINESS);

        final JSONObject event = new JSONObject("{ \"foo\": \"bar\" }");

        Optional<ValidationError> error = EventValidation.forType(et).validate(event);

        assertThat(error.get().getMessage(), equalTo("#: required key [metadata] not found"));
    }

    @Test
    public void validationOfDataChangeEventRequiresExtraFields() {
        final EventType et = buildEventType("some-event-type",
                "{\"type\": \"object\", \"properties\": {\"foo\": {\"type\": \"string\"} }, \"required\": [\"foo\"]}");
        et.setCategory(EventCategory.DATA);

        final JSONObject event = new JSONObject("{ \"data\": { \"foo\": \"bar\" } }");

        Optional<ValidationError> error = EventValidation.forType(et).validate(event);

        assertThat(error.get().getMessage(), equalTo("#: 3 schema violations found\n#: required key [metadata] not found\n#: required key [data_op] not found\n#: required key [data_type] not found"));
    }

    @Test
    public void validationOfDataChangeEventShouldNotAllowAdditionalFieldsAtTheRootLevelObject() {
        final EventType et = buildEventType("some-event-type",
                "{\"type\": \"object\", \"properties\": {\"foo\": {\"type\": \"string\"} }, \"required\": [\"foo\"]}");
        et.setCategory(EventCategory.DATA);

        final JSONObject event = new JSONObject("{ \"data\": { \"foo\": \"bar\" }, \"data_op\": \"C\", \"data_type\": \"some-event-type\", \"metadata\": {}, \"foo\": 1 }");

        Optional<ValidationError> error = EventValidation.forType(et).validate(event);

        assertThat(error.get().getMessage(), equalTo("#: extraneous key [foo] is not permitted"));
    }
}
