package de.zalando.aruha.nakadi.validation;

import static org.junit.Assert.*;

import org.json.JSONObject;

import org.junit.After;
import org.junit.Test;

import de.zalando.aruha.nakadi.domain.EventType;
import de.zalando.aruha.nakadi.domain.EventTypeSchema;
import de.zalando.aruha.nakadi.domain.EventTypeSchema.Type;
import de.zalando.aruha.nakadi.domain.ValidationStrategyConfiguration;

public class JSONSchemaValidationTest {

  // FIXME: once we are fully wired up and other validations exist, this should be removed from here and the
  // registration should be done in a common place.
  static {
    ValidationStrategy.register(EventBodyMustRespectSchema.NAME, new EventBodyMustRespectSchema());
    ValidationStrategy.register(FieldNameMustBeSet.NAME, new FieldNameMustBeSet());
  }

  @Test
  public void eventValidationRegistryShouldResolveTypes() {
    final EventType et = new EventType();
    et.setName("some-event-type");

    final EventType other = new EventType();
    other.setName("some-other-event-type");

    final EventTypeValidator validator = EventValidation.forType(et);
    assertNotNull(validator);
    assertEquals(validator, EventValidation.lookup(et));
    assertNull(EventValidation.lookup(other));
  }

  @Test(expected = IllegalStateException.class)
  public void eventValidationRegistryShouldDenyRespecification() {
    final EventType et = new EventType();
    et.setName("some-event-type");
    EventValidation.forType(et);
    EventValidation.forType(et);
  }

  @Test
  public void eventValidationRegistryShouldAllowRespecificationIfRequestedExplicitly() {
    final EventType et = new EventType();
    et.setName("some-event-type");

    final EventTypeValidator validator = EventValidation.forType(et);
    assertNotNull(validator);
    assertSame(validator, EventValidation.forType(et, true));
  }

  @Test
  public void schemaValidationShouldRespectEventTypeDefinition() {
    final EventType et =
        buildAndRegisterEventType(
            "some-event-type",
            new JSONObject(
                "{\"type\": \"object\", \"properties\": {\"foo\": {\"type\": \"string\"}}, \"required\": [\"foo\"]}"));

    final ValidationStrategyConfiguration vsc1 = new ValidationStrategyConfiguration();
    vsc1.setStrategyName(EventBodyMustRespectSchema.NAME);
    et.getValidationStrategies().add(vsc1);

    final ValidationStrategyConfiguration vsc2 = new ValidationStrategyConfiguration();
    vsc2.setStrategyName(FieldNameMustBeSet.NAME);
    et.getValidationStrategies().add(vsc2);
    EventValidation.forType(et).withConfiguration(vsc1).withConfiguration(vsc2).init();

    final EventTypeValidator validator = EventValidation.lookup(et);

    final JSONObject event =
        new JSONObject(
            "{\"foo\": \"bar\", \"extra\": \"i should be no problem\", \"name\": \"12345\"}");

    validator.validate(event);
  }

  private EventType buildAndRegisterEventType(final String name, final JSONObject schema) {
    final EventType et = new EventType();
    et.setName(name);

    final EventTypeSchema ets = new EventTypeSchema();
    ets.setType(Type.JSON_SCHEMA);
    ets.setSchema(schema);
    et.setEventTypeSchema(ets);

    return et;
  }

  @Test
  public void schemaValidationShouldRespectIgnoreConfigurationMatchRegular() {
    final EventType et =
        buildAndRegisterEventType(
            "some-event-type",
            new JSONObject(
                "{\"type\": \"object\", \"properties\": {\"field-that-will-not-be-found\": {\"type\": \"object\"}, \"event-type\": {\"type\": \"string\"}}, \"required\": [\"field-that-will-not-be-found\", \"event-type\"]}"));

    final ValidationStrategyConfiguration vsc1 = new ValidationStrategyConfiguration();
    vsc1.setStrategyName(EventBodyMustRespectSchema.NAME);
    vsc1.setAdditionalConfiguration(
        new JSONObject(
            "{\"overrides\": [{\"qualifier\": {\"field\": \"event-type\", \"match\" : \"D\"}, \"ignoredProperties\": [\"field-that-will-not-be-found\"]}]}"));
    et.getValidationStrategies().add(vsc1);

    EventValidation.forType(et).withConfiguration(vsc1).init();

    final EventTypeValidator validator = EventValidation.lookup(et);

    final JSONObject event =
        new JSONObject(
            "{\"event-type\" : \"X\", \"extra\": \"i should be no problem\", \"name\": \"12345\", \"field-that-will-not-be-found\": {\"val\": \"i must be present since the matcher will not succeed\"}}");
    validator.validate(event);
  }

  @Test
  public void schemaValidationShouldRespectIgnoreConfigurationMatchQualified() {
    final EventType et =
        buildAndRegisterEventType(
            "some-event-type",
            new JSONObject(
                "{\"type\": \"object\", \"properties\": {\"field-that-will-not-be-found\": {\"type\": \"object\"}, \"event-type\": {\"type\": \"string\"}}, \"required\": [\"field-that-will-not-be-found\", \"event-type\"]}"));

    final ValidationStrategyConfiguration vsc1 = new ValidationStrategyConfiguration();
    vsc1.setStrategyName(EventBodyMustRespectSchema.NAME);
    vsc1.setAdditionalConfiguration(
        new JSONObject(
            "{\"overrides\": [{\"qualifier\": {\"field\": \"event-type\", \"match\" : \"D\"}, \"ignoredProperties\": [\"field-that-will-not-be-found\"]}]}"));
    et.getValidationStrategies().add(vsc1);

    EventValidation.forType(et).withConfiguration(vsc1).init();

    final EventTypeValidator validator = EventValidation.lookup(et);

    final JSONObject event =
        new JSONObject(
            "{\"event-type\" : \"D\", \"field-that-will-not-be-found\": \"some useless value\", \"extra\": \"i should be no problem\", \"name\": \"12345\"}");
    validator.validate(event);
  }

  @After
  public void teardown() {
    EventValidation.reset();
  }
}
