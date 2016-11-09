package org.zalando.nakadi.validation;

import org.everit.json.schema.Schema;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.zalando.nakadi.config.ValidatorConfig;
import org.zalando.nakadi.domain.CompatibilityMode;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.exceptions.InvalidEventTypeException;
import org.zalando.nakadi.utils.EventTypeTestBuilder;

import java.util.Iterator;
import java.util.List;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.zalando.nakadi.utils.TestUtils.readFile;

public class SchemaEvolutionServiceTest {
    private SchemaEvolutionService service;

    @Before
    public void setUp() {
        this.service = new ValidatorConfig().schemaCompatibilityChecker();
    }

    @Test(expected = InvalidEventTypeException.class)
    public void whenChangeCompatibilityModeThenFail() throws Exception {
        final EventTypeTestBuilder builder = EventTypeTestBuilder.builder();
        final EventType oldEventType = builder.compatibilityMode(CompatibilityMode.DEPRECATED).build();
        final EventType newEventType = builder.compatibilityMode(CompatibilityMode.COMPATIBLE).build();

        service.evolve(oldEventType, newEventType);
    }

    @Test(expected = InvalidEventTypeException.class)
    public void whenDeprecatedModeDoNotAllowSchemaChange() throws Exception {
        final EventTypeTestBuilder builder = EventTypeTestBuilder.builder()
                .compatibilityMode(CompatibilityMode.DEPRECATED);
        final EventType oldEventType = builder.schema("{\"type\":\"string\"}").build();
        final EventType newEventType = builder.schema("{\"type\":\"number\"}").build();

        service.evolve(oldEventType, newEventType);
    }

    @Test
    public void whenDeprecatedModeButNoChangesThenKeepVersion() throws Exception {
        final EventTypeTestBuilder builder = EventTypeTestBuilder.builder()
                .compatibilityMode(CompatibilityMode.DEPRECATED)
                .schema("{\"type\":\"string\"}");
        final EventType oldEventType = builder.build();
        final EventType newEventType = builder.build();

        service.evolve(oldEventType, newEventType);

        assertThat(newEventType.getSchema().getVersion().toString(), is(equalTo("1.0.0")));
    }

    @Test
    public void whenCompatibleModeButNoChangesThenKeepVersion() throws Exception {
        final EventTypeTestBuilder builder = EventTypeTestBuilder.builder()
                .compatibilityMode(CompatibilityMode.COMPATIBLE);
        final EventType oldEventType = builder.build();
        final EventType newEventType = builder.build();

        service.evolve(oldEventType, newEventType);

        assertThat(newEventType.getSchema().getVersion().toString(), is(equalTo("1.0.0")));
    }

    @Test
    public void checksJsonSchemaConstraints() throws Exception {
        final JSONArray invalidTestCases = new JSONArray(
                readFile("org/zalando/nakadi/validation/invalid-json-schema-examples.json"));

        for(final Iterator<Object> i = invalidTestCases.iterator(); i.hasNext();) {
            final JSONObject testCase = (JSONObject) i.next();
            final Schema schema = SchemaLoader.load(testCase.getJSONObject("schema"));
            final List<String> errorMessages = testCase
                    .getJSONArray("errors")
                    .toList()
                    .stream()
                    .map(Object::toString)
                    .collect(toList());
            final String description = testCase.getString("description");


            assertThat(description, service.checkConstraints(schema).stream().map(Object::toString).collect(toList()),
                    is(errorMessages));
        }
    }
}
