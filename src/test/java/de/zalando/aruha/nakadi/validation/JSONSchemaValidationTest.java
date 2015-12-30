package de.zalando.aruha.nakadi.validation;

import javax.xml.validation.SchemaFactoryLoader;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONObject;
import org.junit.Test;

import de.zalando.aruha.nakadi.domain.EventType;
import de.zalando.aruha.nakadi.domain.EventTypeSchema;
import de.zalando.aruha.nakadi.domain.EventTypeSchema.Type;
import de.zalando.aruha.nakadi.domain.ValidationStrategyConfiguration;
import scala.NotImplementedError;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class JSONSchemaValidationTest {

	@Test
	public void incomingMessageShouldBeValidated() {
		throw new NotImplementedException();
	}

	@Test
	public void schemaValidationShouldRespectEventTypeDefinition() {
		final EventType et = new EventType();
		et.setName("some-event-type");

		final ValidationStrategyConfiguration vsc1 = new ValidationStrategyConfiguration();
		vsc1.setStrategyName(EventBodyMustRespectSchema.NAME);
		// vsc1.setAdditionalConfiguration(additionalConfiguration);
		et.getValidationStrategies().add(vsc1);

		final ValidationStrategyConfiguration vsc2 = new ValidationStrategyConfiguration();
		vsc2.setStrategyName(FieldNameMustBeSet.NAME);
		et.getValidationStrategies().add(vsc2);

		final EventTypeSchema ets = new EventTypeSchema();
		ets.setType(Type.JSON_SCHEMA);
		ets.setSchema(new JSONObject(
				"{\"type\": \"object\", \"properties\": {\"foo\": {\"type\": \"string\"}}, \"required\": [\"foo\"]}"));
		et.setEventTypeSchema(ets);

		EventValidation.forType(et).withConfiguration(vsc1).withConfiguration(vsc2).init();
		final EventTypeValidator validator = EventValidation.lookup(et);

		final JSONObject event = new JSONObject("{\"fooxx\": \"bar\", \"name\": \"12345\"}");

		validator.validate(event);
	}

	@Test
	public void schemaValidationShouldRespectIgnoreConfiguration() {

	}
}
