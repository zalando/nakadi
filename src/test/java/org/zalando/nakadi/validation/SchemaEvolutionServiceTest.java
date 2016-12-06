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
import org.zalando.nakadi.utils.EventTypeTestBuilder;
import org.zalando.nakadi.validation.schema.diff.SchemaDiff;
import org.zalando.nakadi.validation.schema.SchemaEvolutionConstraint;
import org.zalando.nakadi.validation.schema.SchemaEvolutionIncompatibility;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.zalando.nakadi.domain.SchemaChange.Type.TITLE_CHANGED;
import static org.zalando.nakadi.domain.Version.Level.MAJOR;
import static org.zalando.nakadi.domain.Version.Level.MINOR;
import static org.zalando.nakadi.domain.Version.Level.PATCH;
import static org.zalando.nakadi.utils.TestUtils.readFile;

public class SchemaEvolutionServiceTest {
    private SchemaEvolutionService service;
    private final SchemaEvolutionConstraint evolutionConstraint = mock(SchemaEvolutionConstraint.class);
    private final Map<SchemaChange.Type, Version.Level> changeToLevel = mock(HashMap.class);
    private final Map<SchemaChange.Type, String> errorMessages = mock(HashMap.class);
    private final SchemaDiff schemaDiff = mock(SchemaDiff.class);

    @Before
    public void setUp() throws IOException {
        final List<SchemaEvolutionConstraint> evolutionConstraints= Lists.newArrayList(evolutionConstraint);
        final JSONObject metaSchemaJson = new JSONObject(Resources.toString(Resources.getResource("schema.json"),
                Charsets.UTF_8));
        final Schema metaSchema = SchemaLoader.load(metaSchemaJson);
        this.service = new SchemaEvolutionService(metaSchema, evolutionConstraints, schemaDiff, changeToLevel,
                errorMessages);

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
    public void whenPatchChangesBumpVersion() throws Exception {
        final EventTypeTestBuilder builder = EventTypeTestBuilder.builder();
        final EventType oldEventType = builder.build();
        final EventType newEventType = builder.build();

        Mockito.doReturn(Optional.empty()).when(evolutionConstraint).validate(oldEventType, newEventType);
        Mockito.doReturn(PATCH).when(changeToLevel).get(any());
        Mockito.doReturn(Lists.newArrayList(new SchemaChange(TITLE_CHANGED, "#/"))).when(schemaDiff)
                .collectChanges(any(), any());

        final EventType eventType = service.evolve(oldEventType, newEventType);

        assertThat(eventType.getSchema().getVersion(), is(equalTo(new Version("1.0.1"))));

        verify(evolutionConstraint).validate(oldEventType, newEventType);
    }

    @Test
    public void whenMinorChangesBumpVersion() throws Exception {
        final EventTypeTestBuilder builder = EventTypeTestBuilder.builder();
        final EventType oldEventType = builder.build();
        final EventType newEventType = builder.build();

        Mockito.doReturn(Optional.empty()).when(evolutionConstraint).validate(oldEventType, newEventType);
        Mockito.doReturn(MINOR).when(changeToLevel).get(any());
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
        Mockito.doReturn(MAJOR).when(changeToLevel).get(any());
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
        Mockito.doReturn(MAJOR).when(changeToLevel).get(any());
        Mockito.doReturn(Lists.newArrayList(new SchemaChange(TITLE_CHANGED, "#/"))).when(schemaDiff)
                .collectChanges(any(), any());

        final EventType eventType = service.evolve(oldEventType, newEventType);

        assertThat(eventType.getSchema().getVersion(), is(equalTo(new Version("2.0.0"))));

        verify(evolutionConstraint).validate(oldEventType, newEventType);
    }

    @Test
    public void checksJsonSchemaConstraints() throws Exception {
        final JSONArray testCases = new JSONArray(
                readFile("org/zalando/nakadi/validation/invalid-json-schema-examples.json"));

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

            assertThat(description, service.collectIncompatibilities(schemaJson).stream().map(Object::toString)
                            .collect(toList()), is(errorMessages));
        }
    }
}
