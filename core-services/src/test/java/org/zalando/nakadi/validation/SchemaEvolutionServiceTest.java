package org.zalando.nakadi.validation;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.io.Resources;
import org.everit.json.schema.Schema;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.zalando.nakadi.domain.CompatibilityMode;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.EventTypeSchema;
import org.zalando.nakadi.domain.EventTypeSchemaBase;
import org.zalando.nakadi.domain.SchemaChange;
import org.zalando.nakadi.domain.Version;
import org.zalando.nakadi.exceptions.runtime.SchemaEvolutionException;
import org.zalando.nakadi.service.AvroSchemaCompatibility;
import org.zalando.nakadi.service.SchemaEvolutionService;
import org.zalando.nakadi.utils.EventTypeTestBuilder;
import org.zalando.nakadi.utils.TestUtils;
import org.zalando.nakadi.validation.schema.SchemaEvolutionConstraint;
import org.zalando.nakadi.validation.schema.SchemaEvolutionIncompatibility;
import org.zalando.nakadi.validation.schema.diff.SchemaDiff;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
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
import static org.zalando.nakadi.domain.Version.Level.PATCH;

public class SchemaEvolutionServiceTest {
    private SchemaEvolutionService service;
    private final SchemaEvolutionConstraint evolutionConstraint = Mockito.mock(SchemaEvolutionConstraint.class);
    private final Map<SchemaChange.Type, String> errorMessages = Mockito.mock(HashMap.class);
    private final BiFunction<SchemaChange.Type, CompatibilityMode, Version.Level> levelResolver =
            Mockito.mock(BiFunction.class);
    private final SchemaDiff schemaDiff = Mockito.mock(SchemaDiff.class);
    private final AvroSchemaCompatibility avroSchemaCompatibility = Mockito.mock(AvroSchemaCompatibility.class);

    @Before
    public void setUp() throws IOException {
        final List<SchemaEvolutionConstraint> evolutionConstraints = Lists.newArrayList(evolutionConstraint);
        final JSONObject metaSchemaJson = new JSONObject(Resources.toString(Resources.getResource("schema.json"),
                Charsets.UTF_8));
        final Schema metaSchema = SchemaLoader.load(metaSchemaJson);
        this.service = new SchemaEvolutionService(metaSchema, evolutionConstraints, schemaDiff, levelResolver,
                errorMessages, avroSchemaCompatibility);

        Mockito.doReturn("error").when(errorMessages).get(any());
    }

    @Test(expected = SchemaEvolutionException.class)
    public void checkEvolutionConstraints() {
        final EventTypeTestBuilder builder = EventTypeTestBuilder.builder();
        final EventType oldEventType = builder.build();
        final EventType newEventType = builder.build();

        final Optional<SchemaEvolutionIncompatibility> incompatibility = Optional.of(
                new SchemaEvolutionIncompatibility("incompatible"));
        Mockito.doReturn(incompatibility).when(evolutionConstraint).validate(oldEventType, newEventType);

        service.evolve(oldEventType, newEventType);
    }

    @Test
    public void whenNoChangesKeepVersion() {
        final EventTypeTestBuilder builder = EventTypeTestBuilder.builder();
        final EventType oldEventType = builder.build();
        final EventType newEventType = builder.build();

        Mockito.doReturn(Optional.empty()).when(evolutionConstraint).validate(oldEventType, newEventType);

        final EventType eventType = service.evolve(oldEventType, newEventType);

        Assert.assertThat(eventType.getSchema().getVersion(), is(equalTo("1.0.0")));

        Mockito.verify(evolutionConstraint).validate(oldEventType, newEventType);
    }

    @Test
    public void whenNoSemanticalChangesButTextualChangedBumpPatch() {
        final EventTypeTestBuilder builder = EventTypeTestBuilder.builder();
        final EventType oldEventType = builder.schema("{\"default\":\"this\"}").build();
        final EventType newEventType = builder.schema("{\"default\":\"that\"}").build();

        Mockito.doReturn(Optional.empty()).when(evolutionConstraint).validate(oldEventType, newEventType);

        final EventType eventType = service.evolve(oldEventType, newEventType);

        Assert.assertThat(eventType.getSchema().getVersion(), is(equalTo("1.0.1")));

        Mockito.verify(evolutionConstraint).validate(oldEventType, newEventType);
    }

    @Test
    public void whenNoSchemaChangeThenSchemaCreatedAtUnchanged() {
        final EventTypeTestBuilder builder = EventTypeTestBuilder.builder();
        final EventType oldEventType = builder.build();
        final EventType newEventType = builder.build();

        Mockito.doReturn(Optional.empty()).when(evolutionConstraint).validate(oldEventType, newEventType);
        final EventType eventType = service.evolve(oldEventType, newEventType);
        Assert.assertEquals(eventType.getSchema().getCreatedAt(), oldEventType.getSchema().getCreatedAt());
    }

    @Test
    public void whenPatchChangesBumpVersion() {
        final EventTypeTestBuilder builder = EventTypeTestBuilder.builder();
        final EventType oldEventType = builder.build();
        final EventType newEventType = builder.build();

        Mockito.doReturn(Optional.empty()).when(evolutionConstraint).validate(oldEventType, newEventType);
        Mockito.when(levelResolver.apply(any(), eq(CompatibilityMode.COMPATIBLE))).thenReturn(PATCH);
        Mockito.doReturn(Lists.newArrayList(new SchemaChange(TITLE_CHANGED, "#/"))).when(schemaDiff)
                .collectChanges(any(), any());

        final EventType eventType = service.evolve(oldEventType, newEventType);

        Assert.assertThat(eventType.getSchema().getVersion(), is(equalTo("1.0.1")));

        Mockito.verify(evolutionConstraint).validate(oldEventType, newEventType);
    }

    @Test
    public void whenMinorChangesBumpVersion() {
        final EventTypeTestBuilder builder = EventTypeTestBuilder.builder();
        final EventType oldEventType = builder.build();
        final EventType newEventType = builder.build();

        Mockito.doReturn(Optional.empty()).when(evolutionConstraint).validate(oldEventType, newEventType);
        Mockito.when(levelResolver.apply(any(), eq(CompatibilityMode.COMPATIBLE))).thenReturn(MINOR);
        Mockito.doReturn(Lists.newArrayList(new SchemaChange(TITLE_CHANGED, "#/"))).when(schemaDiff)
                .collectChanges(any(), any());

        final EventType eventType = service.evolve(oldEventType, newEventType);

        Assert.assertThat(eventType.getSchema().getVersion(), is(equalTo("1.1.0")));

        Mockito.verify(evolutionConstraint).validate(oldEventType, newEventType);
    }

    @Test(expected = SchemaEvolutionException.class)
    public void whenCompatibleModeDoNotAllowMajorChanges() {
        final EventTypeTestBuilder builder = EventTypeTestBuilder.builder();
        final EventType oldEventType = builder.build();
        final EventType newEventType = builder.build();

        Mockito.doReturn(Optional.empty()).when(evolutionConstraint).validate(oldEventType, newEventType);
        Mockito.when(levelResolver.apply(any(), eq(CompatibilityMode.COMPATIBLE))).thenReturn(MAJOR);
        Mockito.doReturn(Lists.newArrayList(new SchemaChange(TITLE_CHANGED, "#/"))).when(schemaDiff)
                .collectChanges(any(), any());

        service.evolve(oldEventType, newEventType);
    }

    @Test
    public void whenIncompatibleModeAllowMajorChanges() {
        final EventTypeTestBuilder builder = EventTypeTestBuilder.builder().compatibilityMode(CompatibilityMode.NONE);
        final EventType oldEventType = builder.build();
        final EventType newEventType = builder.build();

        Mockito.doReturn(Optional.empty()).when(evolutionConstraint).validate(oldEventType, newEventType);
        Mockito.when(levelResolver.apply(any(), eq(CompatibilityMode.NONE))).thenReturn(MAJOR);
        Mockito.doReturn(Lists.newArrayList(new SchemaChange(TITLE_CHANGED, "#/"))).when(schemaDiff)
                .collectChanges(any(), any());

        final EventType eventType = service.evolve(oldEventType, newEventType);

        Assert.assertThat(eventType.getSchema().getVersion(), is(equalTo("2.0.0")));

        Mockito.verify(evolutionConstraint).validate(oldEventType, newEventType);
    }

    @Test
    public void compatibilityModeMigrationAllowedChanges() {
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
            Mockito.when(levelResolver.apply(any(), eq(CompatibilityMode.FORWARD))).thenReturn(MINOR);
            Mockito.doReturn(Lists.newArrayList(new SchemaChange(changeType, "#/"))).when(schemaDiff)
                    .collectChanges(any(), any());

            final EventType eventType;
            try {
                eventType = service.evolve(oldEventType, newEventType);
                Assert.assertThat(eventType.getSchema().getVersion(), is(equalTo("1.1.0")));
            } catch (final SchemaEvolutionException e) {
                Assert.fail();
            }
        });

        notAllowedChanges.forEach(changeType -> {
            Mockito.doReturn(Lists.newArrayList(new SchemaChange(changeType, "#/"))).when(schemaDiff)
                    .collectChanges(any(), any());

            try {
                service.evolve(oldEventType, newEventType);
                Assert.fail();
            } catch (final SchemaEvolutionException e) {
            }
        });
    }

    @Test
    public void checksJsonSchemaConstraints() throws Exception {
        final JSONArray testCases = new JSONArray(
                TestUtils.readFile("invalid-json-schema-examples.json"));

        for (final Object testCaseObject : testCases) {
            final JSONObject testCase = (JSONObject) testCaseObject;
            final JSONObject schemaJson = testCase.getJSONObject("schema");
            final List<String> errorMessages = testCase
                    .getJSONArray("errors")
                    .toList()
                    .stream()
                    .map(Object::toString)
                    .collect(toList());
            final String description = testCase.getString("description");

            Assert.assertThat(description, service.collectIncompatibilities(schemaJson).stream().map(Object::toString)
                    .collect(toList()), is(errorMessages));
        }
    }

    @Test
    public void testVersionOnTypeChangeToAvro(){
        final EventType oldEventType = EventTypeTestBuilder.builder().
                compatibilityMode(CompatibilityMode.NONE).build();

        final var schema = new EventTypeSchema(new EventTypeSchemaBase(EventTypeSchemaBase.Type.AVRO_SCHEMA, ""),
                "", TestUtils.randomDate());
        final EventType newEventType =
                EventTypeTestBuilder.builder().
                compatibilityMode(CompatibilityMode.NONE).schema(schema).build();

        Mockito.when(levelResolver.apply(any(), eq(CompatibilityMode.NONE))).thenReturn(MAJOR);

        final EventType eventType = service.evolve(oldEventType, newEventType);

        Assert.assertThat(eventType.getSchema().getVersion(), is(equalTo("2.0.0")));
    }

    @Test(expected = SchemaEvolutionException.class)
    public void testVersionOnTypeChangeToJson(){
        final var schema = new EventTypeSchema(new EventTypeSchemaBase(EventTypeSchemaBase.Type.AVRO_SCHEMA, ""),
                "2.1.1", TestUtils.randomDate());

        final EventType oldEventType =
                EventTypeTestBuilder.builder().
                compatibilityMode(CompatibilityMode.NONE).schema(schema).build();
        final EventType newEventType =
                EventTypeTestBuilder.builder().compatibilityMode(CompatibilityMode.NONE).build();

        service.evolve(oldEventType, newEventType);
    }
}
