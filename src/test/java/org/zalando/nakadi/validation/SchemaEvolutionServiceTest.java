package org.zalando.nakadi.validation;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.io.Resources;
import org.everit.json.schema.Schema;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.zalando.nakadi.domain.CompatibilityMode;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.SchemaChange;
import org.zalando.nakadi.domain.Version;
import org.zalando.nakadi.exceptions.InvalidEventTypeException;
import org.zalando.nakadi.exceptions.runtime.UnexpectedSchemaChangeException;
import org.zalando.nakadi.utils.EventTypeTestBuilder;
import org.zalando.nakadi.validation.schema.SchemaEvolutionConstraint;
import org.zalando.nakadi.validation.schema.SchemaEvolutionIncompatibility;
import org.zalando.nakadi.validation.schema.diff.SchemaDiff;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.zalando.nakadi.domain.SchemaChange.Type.ADDITIONAL_ITEMS_CHANGED;
import static org.zalando.nakadi.domain.SchemaChange.Type.ADDITIONAL_PROPERTIES_CHANGED;
import static org.zalando.nakadi.domain.SchemaChange.Type.ATTRIBUTE_VALUE_CHANGED;
import static org.zalando.nakadi.domain.SchemaChange.Type.COMPOSITION_METHOD_CHANGED;
import static org.zalando.nakadi.domain.SchemaChange.Type.DEPENDENCY_ARRAY_CHANGED;
import static org.zalando.nakadi.domain.SchemaChange.Type.DEPENDENCY_SCHEMA_CHANGED;
import static org.zalando.nakadi.domain.SchemaChange.Type.DEPENDENCY_SCHEMA_REMOVED;
import static org.zalando.nakadi.domain.SchemaChange.Type.DESCRIPTION_CHANGED;
import static org.zalando.nakadi.domain.SchemaChange.Type.ENUM_ARRAY_CHANGED;
import static org.zalando.nakadi.domain.SchemaChange.Type.ID_CHANGED;
import static org.zalando.nakadi.domain.SchemaChange.Type.NUMBER_OF_ITEMS_CHANGED;
import static org.zalando.nakadi.domain.SchemaChange.Type.PROPERTIES_ADDED;
import static org.zalando.nakadi.domain.SchemaChange.Type.PROPERTY_REMOVED;
import static org.zalando.nakadi.domain.SchemaChange.Type.REQUIRED_ARRAY_CHANGED;
import static org.zalando.nakadi.domain.SchemaChange.Type.REQUIRED_ARRAY_EXTENDED;
import static org.zalando.nakadi.domain.SchemaChange.Type.SCHEMA_REMOVED;
import static org.zalando.nakadi.domain.SchemaChange.Type.SUB_SCHEMA_CHANGED;
import static org.zalando.nakadi.domain.SchemaChange.Type.TITLE_CHANGED;
import static org.zalando.nakadi.domain.SchemaChange.Type.TYPE_CHANGED;
import static org.zalando.nakadi.domain.Version.Level.MAJOR;
import static org.zalando.nakadi.domain.Version.Level.MINOR;
import static org.zalando.nakadi.domain.Version.Level.NO_CHANGES;
import static org.zalando.nakadi.domain.Version.Level.PATCH;
import static org.zalando.nakadi.utils.TestUtils.readFile;

public class SchemaEvolutionServiceTest {
    private SchemaEvolutionService service;
    private final SchemaEvolutionConstraint evolutionConstraint = mock(SchemaEvolutionConstraint.class);
    private final Map<SchemaChange.Type, Version.Level> compatibleChanges = mock(HashMap.class);
    private final Map<SchemaChange.Type, Version.Level> forwardChanges = mock(HashMap.class);
    private final Map<SchemaChange.Type, String> errorMessages = mock(HashMap.class);
    private final SchemaDiff schemaDiff = mock(SchemaDiff.class);

    @Before
    public void setUp() throws IOException {
        final List<SchemaEvolutionConstraint> evolutionConstraints= Lists.newArrayList(evolutionConstraint);
        final Map<CompatibilityMode, Schema> schemaValidator = new HashMap<>();
        schemaValidator.put(CompatibilityMode.COMPATIBLE, loadSchema("compatible-schema.json"));
        schemaValidator.put(CompatibilityMode.FORWARD, loadSchema("forward-schema.json"));
        schemaValidator.put(CompatibilityMode.NONE, loadSchema("none-schema.json"));
        this.service = new SchemaEvolutionService(schemaValidator, evolutionConstraints, schemaDiff, compatibleChanges,
                forwardChanges, errorMessages);

        Mockito.doReturn("error").when(errorMessages).get(any());
    }

    @Test(expected = InvalidEventTypeException.class)
    public void checkEvolutionConstraints() throws Exception {
        final EventTypeTestBuilder builder = EventTypeTestBuilder.builder();
        final EventType oldEventType = builder.build();
        final EventType newEventType = builder.build();

        final Optional<SchemaEvolutionIncompatibility> incompatibility = Optional.of(
                new SchemaEvolutionIncompatibility("incompatible"));
        Mockito.doReturn(incompatibility).when(evolutionConstraint).validate(oldEventType, newEventType);

        service.evolve(oldEventType, newEventType);
    }

    @Test
    public void whenNoChangesKeepVersion() throws Exception {
        final EventTypeTestBuilder builder = EventTypeTestBuilder.builder();
        final EventType oldEventType = builder.build();
        final EventType newEventType = builder.build();

        Mockito.doReturn(Optional.empty()).when(evolutionConstraint).validate(oldEventType, newEventType);

        final EventType eventType = service.evolve(oldEventType, newEventType);

        assertThat(eventType.getSchema().getVersion(), is(equalTo(new Version("1.0.0"))));

        verify(evolutionConstraint).validate(oldEventType, newEventType);
    }

    @Test
    public void whenNoSemanticalChangesButTextualChangedBumpPatch() throws Exception {
        final EventTypeTestBuilder builder = EventTypeTestBuilder.builder();
        final EventType oldEventType = builder.schema("{\"default\":\"this\"}").build();
        final EventType newEventType = builder.schema("{\"default\":\"that\"}").build();

        Mockito.doReturn(Optional.empty()).when(evolutionConstraint).validate(oldEventType, newEventType);

        final EventType eventType = service.evolve(oldEventType, newEventType);

        assertThat(eventType.getSchema().getVersion(), is(equalTo(new Version("1.0.1"))));

        verify(evolutionConstraint).validate(oldEventType, newEventType);
    }

    @Test
    public void whenPatchChangesBumpVersion() throws Exception {
        final EventTypeTestBuilder builder = EventTypeTestBuilder.builder();
        final EventType oldEventType = builder.build();
        final EventType newEventType = builder.build();

        Mockito.doReturn(Optional.empty()).when(evolutionConstraint).validate(oldEventType, newEventType);
        Mockito.doReturn(PATCH).when(compatibleChanges).get(any());
        Mockito.doReturn(Lists.newArrayList(new SchemaChange(TITLE_CHANGED, "#/"))).when(schemaDiff)
                .collectChanges(any(), any());

        final EventType eventType = service.evolve(oldEventType, newEventType);

        assertThat(eventType.getSchema().getVersion(), is(equalTo(new Version("1.0.1"))));

        verify(evolutionConstraint).validate(oldEventType, newEventType);
    }

    @Test(expected = UnexpectedSchemaChangeException.class)
    public void whenNoChangesDetectedButSchemaIsDifferent() throws Exception {
        final EventTypeTestBuilder builder = EventTypeTestBuilder.builder();
        final EventType oldEventType = builder.schema("{}").build();
        final EventType newEventType = builder.schema("{\"example\":\"something\"}").build();

        Mockito.doReturn(Optional.empty()).when(evolutionConstraint).validate(oldEventType, newEventType);
        Mockito.doReturn(NO_CHANGES).when(compatibleChanges).get(any());
        Mockito.doReturn(Lists.newArrayList(new SchemaChange(TITLE_CHANGED, "#/"))).when(schemaDiff)
                .collectChanges(any(), any());

        service.evolve(oldEventType, newEventType);
    }

    @Test
    public void whenMinorChangesBumpVersion() throws Exception {
        final EventTypeTestBuilder builder = EventTypeTestBuilder.builder();
        final EventType oldEventType = builder.build();
        final EventType newEventType = builder.build();

        Mockito.doReturn(Optional.empty()).when(evolutionConstraint).validate(oldEventType, newEventType);
        Mockito.doReturn(MINOR).when(compatibleChanges).get(any());
        Mockito.doReturn(Lists.newArrayList(new SchemaChange(TITLE_CHANGED, "#/"))).when(schemaDiff)
                .collectChanges(any(), any());

        final EventType eventType = service.evolve(oldEventType, newEventType);

        assertThat(eventType.getSchema().getVersion(), is(equalTo(new Version("1.1.0"))));

        verify(evolutionConstraint).validate(oldEventType, newEventType);
    }

    @Test(expected = InvalidEventTypeException.class)
    public void whenCompatibleModeDoNotAllowMajorChanges() throws Exception {
        final EventTypeTestBuilder builder = EventTypeTestBuilder.builder();
        final EventType oldEventType = builder.build();
        final EventType newEventType = builder.build();

        Mockito.doReturn(Optional.empty()).when(evolutionConstraint).validate(oldEventType, newEventType);
        Mockito.doReturn(MAJOR).when(compatibleChanges).get(any());
        Mockito.doReturn(Lists.newArrayList(new SchemaChange(TITLE_CHANGED, "#/"))).when(schemaDiff)
                .collectChanges(any(), any());

        service.evolve(oldEventType, newEventType);
    }

    @Test
    public void whenIncompatibleModeAllowMajorChanges() throws Exception {
        final EventTypeTestBuilder builder = EventTypeTestBuilder.builder().compatibilityMode(CompatibilityMode.NONE);
        final EventType oldEventType = builder.build();
        final EventType newEventType = builder.build();

        Mockito.doReturn(Optional.empty()).when(evolutionConstraint).validate(oldEventType, newEventType);
        Mockito.doReturn(MAJOR).when(forwardChanges).get(any());
        Mockito.doReturn(Lists.newArrayList(new SchemaChange(TITLE_CHANGED, "#/"))).when(schemaDiff)
                .collectChanges(any(), any());

        final EventType eventType = service.evolve(oldEventType, newEventType);

        assertThat(eventType.getSchema().getVersion(), is(equalTo(new Version("2.0.0"))));

        verify(evolutionConstraint).validate(oldEventType, newEventType);
    }

    @Test
    public void compatibilityModeMigrationAllowedChanges() throws Exception {
        final EventTypeTestBuilder builder = EventTypeTestBuilder.builder()
                .compatibilityMode(CompatibilityMode.FORWARD);
        final EventType oldEventType = builder.build();
        final EventType newEventType = builder.compatibilityMode(CompatibilityMode.COMPATIBLE).build();

        Mockito.doReturn(Optional.empty()).when(evolutionConstraint).validate(oldEventType, newEventType);

        final List<SchemaChange.Type> allowedChanges = Lists.newArrayList(
                DESCRIPTION_CHANGED,
                TITLE_CHANGED,
                PROPERTIES_ADDED,
                REQUIRED_ARRAY_EXTENDED,
                ADDITIONAL_PROPERTIES_CHANGED,
                ADDITIONAL_ITEMS_CHANGED);

        final List<SchemaChange.Type> notAllowedChanges = Lists.newArrayList(
                ID_CHANGED,
                SCHEMA_REMOVED,
                TYPE_CHANGED,
                NUMBER_OF_ITEMS_CHANGED,
                PROPERTY_REMOVED,
                DEPENDENCY_ARRAY_CHANGED,
                DEPENDENCY_SCHEMA_CHANGED,
                COMPOSITION_METHOD_CHANGED,
                ATTRIBUTE_VALUE_CHANGED,
                ENUM_ARRAY_CHANGED,
                SUB_SCHEMA_CHANGED,
                DEPENDENCY_SCHEMA_REMOVED,
                REQUIRED_ARRAY_CHANGED);

        allowedChanges.forEach(changeType -> {
            Mockito.doReturn(MINOR).when(forwardChanges).get(any());
            Mockito.doReturn(Lists.newArrayList(new SchemaChange(changeType, "#/"))).when(schemaDiff)
                    .collectChanges(any(), any());

            final EventType eventType;
            try {
                eventType = service.evolve(oldEventType, newEventType);
                assertThat(eventType.getSchema().getVersion(), is(equalTo(new Version("1.1.0"))));
            } catch (final InvalidEventTypeException e) {
                fail();
            }
        });

        notAllowedChanges.forEach(changeType -> {
            Mockito.doReturn(Lists.newArrayList(new SchemaChange(changeType, "#/"))).when(schemaDiff)
                    .collectChanges(any(), any());

            try {
                service.evolve(oldEventType, newEventType);
                fail();
            } catch (final InvalidEventTypeException e) {
            }
        });
    }

    @Test
    public void checksCompatibleJsonSchemaConstraints() throws Exception {
        final JSONArray testCases = new JSONArray(
                readFile("org/zalando/nakadi/validation/invalid-compatible-json-schema-examples.json"));
        final EventType eventType = EventTypeTestBuilder.builder().compatibilityMode(CompatibilityMode.COMPATIBLE)
                .build();


        runTestExamples(testCases, eventType);
    }

    @Test
    public void checksForwardJsonSchemaConstraints() throws Exception {
        final JSONArray testCases = new JSONArray(
                readFile("org/zalando/nakadi/validation/invalid-forward-json-schema-examples.json"));
        final EventType eventType = EventTypeTestBuilder.builder().compatibilityMode(CompatibilityMode.FORWARD)
                .build();

        runTestExamples(testCases, eventType);
    }

    @Test
    public void checksNoneJsonSchemaConstraints() throws Exception {
        final JSONArray testCases = new JSONArray(
                readFile("org/zalando/nakadi/validation/invalid-none-json-schema-examples.json"));
        final EventType eventType = EventTypeTestBuilder.builder().compatibilityMode(CompatibilityMode.NONE)
                .build();

        runTestExamples(testCases, eventType);
    }

    private void runTestExamples(final JSONArray testCases, final EventType eventType) {
        for(final Object testCaseObject : testCases) {
            final JSONObject testCase = (JSONObject) testCaseObject;
            final JSONObject schemaJson = testCase.getJSONObject("schema");
            final List<String> errorMessages = testCase
                    .getJSONArray("errors")
                    .toList()
                    .stream()
                    .map(Object::toString)
                    .collect(toList());
            final String description = testCase.getString("description");

            assertThat(description, service.collectIncompatibilities(eventType.getCompatibilityMode(), schemaJson)
                    .stream()
                    .map(Object::toString)
                    .collect(toList()), is(errorMessages));
        }
    }

    private Schema loadSchema(final String filename) throws IOException {
        final JSONObject metaSchemaJson = new JSONObject(
                Resources.toString(Resources.getResource(filename), Charsets.UTF_8)
        );
        return SchemaLoader.load(metaSchemaJson);
    }
}
