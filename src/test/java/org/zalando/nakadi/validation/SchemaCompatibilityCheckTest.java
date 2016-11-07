package org.zalando.nakadi.validation;

import org.everit.json.schema.Schema;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.zalando.nakadi.config.ValidatorConfig;

import java.util.Iterator;
import java.util.List;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.zalando.nakadi.utils.TestUtils.readFile;

public class SchemaCompatibilityCheckTest {
    private SchemaCompatibilityChecker checker;

    @Before
    public void setUp() {
        this.checker = new ValidatorConfig().schemaCompatibilityChecker();
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


            assertThat(description, checker.checkConstraints(schema).stream().map(Object::toString).collect(toList()),
                    is(errorMessages));
        }
    }
}
