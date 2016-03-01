package de.zalando.aruha.nakadi.validation;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsonorg.JsonOrgModule;
import de.zalando.aruha.nakadi.domain.EventType;
import de.zalando.aruha.nakadi.domain.ValidationStrategyConfiguration;
import org.everit.json.schema.Schema;
import org.everit.json.schema.ValidationException;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class EventBodyMustRespectSchema extends ValidationStrategy {

    public static final String NAME = "schema-validation";

    private static final Function<OverrideDefinition, QualifiedJSONSchemaValidator> toQualifiedJSONSchemaValidator =
        t -> {
        final JSONSchemaValidator jsv = new JSONSchemaValidator(t.getEffectiveSchema());
        return new QualifiedJSONSchemaValidator(t.getQualifier(), jsv);
    };

    @Override
    public EventValidator materialize(final EventType eventType, final ValidationStrategyConfiguration vsc) {

        final JSONSchemaValidator defaultSchemaValidator = new JSONSchemaValidator(
                new JSONObject(eventType.getSchema().getSchema()));
        if (vsc.getAdditionalConfiguration() == null) {
            return defaultSchemaValidator;
        }

        final ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JsonOrgModule());

        final Function<OverrideDefinition, OverrideDefinition> enhanceWithQualifiedSchema = definition -> {
            final JSONObject schema = new JSONObject(eventType.getSchema().getSchema());
            final JSONObject copy = new JSONObject(schema, JSONObject.getNames(schema));

            definition.getIgnoredProperties().forEach(name -> {
                copy.getJSONObject("properties").remove(name);

                final JSONArray array = copy.getJSONArray("required");
                final int idx = findElement(array, name);
                if (idx > -1) {
                    array.remove(idx);
                }

            });
            definition.setEffectiveSchema(copy);
            return definition;
        };

        final List<QualifiedJSONSchemaValidator> qualifiedValidators = mapper.convertValue(
                                                                                 vsc.getAdditionalConfiguration(),
                                                                                 SchemaValidationOverrides.class)
                                                                             .getOverrides().stream()
                                                                             .map(enhanceWithQualifiedSchema)
                                                                             .map(toQualifiedJSONSchemaValidator)
                                                                             .collect(Collectors.toList());

        return new QualifiedJSONSchemaValidationChain(qualifiedValidators, defaultSchemaValidator);
    }

    private static int findElement(final JSONArray array, final String element) {

        for (int i = 0; i < array.length(); i++) {
            if (element.equals(array.getString(i))) {
                return i;

            }
        }

        return -1;
    }

}

class SchemaValidationOverrides {
    private List<OverrideDefinition> overrides;

    public List<OverrideDefinition> getOverrides() {
        return overrides;
    }

    public void setOverrides(final List<OverrideDefinition> overrides) {
        this.overrides = overrides;
    }
}

class Qualifier {
    private String field;
    private String match;

    public String getField() {
        return field;
    }

    public void setField(final String field) {
        this.field = field;
    }

    public String getMatch() {
        return match;
    }

    public void setMatch(final String match) {
        this.match = match;
    }
}

class OverrideDefinition {
    private Qualifier qualifier;
    private List<String> ignoredProperties;
    private JSONObject effectiveSchema;

    public Qualifier getQualifier() {
        return qualifier;
    }

    public void setQualifier(final Qualifier qualifier) {
        this.qualifier = qualifier;
    }

    public List<String> getIgnoredProperties() {
        return ignoredProperties;
    }

    public void setIgnoredProperties(final List<String> ignore) {
        this.ignoredProperties = ignore;
    }

    public JSONObject getEffectiveSchema() {
        return effectiveSchema;
    }

    public void setEffectiveSchema(final JSONObject effectiveSchema) {
        this.effectiveSchema = effectiveSchema;
    }
}

class JSONSchemaValidator implements EventValidator {

    private static final Logger LOG = LoggerFactory.getLogger(JSONSchemaValidator.class);

    private final Schema schema;
    private final JSONObject effectiveSchema;

    public JSONSchemaValidator(final JSONObject effectiveSchema) {
        this.effectiveSchema = effectiveSchema;
        schema = SchemaLoader.load(effectiveSchema);
    }

    @Override
    public Optional<ValidationError> accepts(final JSONObject event) {
        try {
            schema.validate(event);

            return Optional.empty();
        } catch (final ValidationException e) {
            StringBuilder builder = new StringBuilder();
            collectErrorMessages(e, builder);

            return Optional.of(new ValidationError(builder.toString()));
        }
    }

    private void collectErrorMessages(ValidationException e, StringBuilder builder) {
        builder.append(e.getMessage());

        e.getCausingExceptions().stream()
                .forEach(causingException -> {
                    builder.append("\n");
                    collectErrorMessages(causingException, builder);
                });
    }
}

class QualifiedJSONSchemaValidator implements EventValidator {
    private Qualifier qualifier;
    private JSONSchemaValidator validator;

    public QualifiedJSONSchemaValidator(final Qualifier qualifier, final JSONSchemaValidator jsv) {
        this.qualifier = qualifier;
        this.validator = jsv;
    }

    public Qualifier getQualifier() {
        return qualifier;
    }

    public void setQualifier(final Qualifier qualifier) {
        this.qualifier = qualifier;
    }

    public JSONSchemaValidator getValidator() {
        return validator;
    }

    public void setValidator(final JSONSchemaValidator validator) {
        this.validator = validator;
    }

    @Override
    public Optional<ValidationError> accepts(final JSONObject event) {
        return validator.accepts(event);
    }
}

class QualifiedJSONSchemaValidationChain implements EventValidator {
    private final List<QualifiedJSONSchemaValidator> qualifiedValidators;
    private final JSONSchemaValidator defaultSchemaValidator;

    public QualifiedJSONSchemaValidationChain(final List<QualifiedJSONSchemaValidator> qualifiedValidators,
            final JSONSchemaValidator defaultSchemaValidator) {
        this.qualifiedValidators = qualifiedValidators;
        this.defaultSchemaValidator = defaultSchemaValidator;
    }

    @Override
    public Optional<ValidationError> accepts(final JSONObject event) {
        final Predicate<QualifiedJSONSchemaValidator> matchingQualifier = it ->
                it.getQualifier().getMatch().equals(event.getString(it.getQualifier().getField()));

        final EventValidator validator = qualifiedValidators.stream().filter(matchingQualifier)
                                                            .map(EventValidator.class::cast).findFirst().orElse(
                                                                defaultSchemaValidator);
        return validator.accepts(event);
    }
}
