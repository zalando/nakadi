package org.zalando.nakadi.validation;

import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.zalando.nakadi.utils.TestUtils.readFile;

public class SchemaCompatibilityCheckTest {
    private SchemaCompatibilityChecker checker;

    @Before
    public void setUp() {
        this.checker = new SchemaCompatibilityChecker();
    }

    @Test
    public void checksJsonSchemaConstraints() throws Exception {
        final JSONArray invalidTestCases = new JSONArray(
                readFile("org/zalando/nakadi/validation/invalid-json-schema-examples.json"));

        for(final Iterator<Object> i = invalidTestCases.iterator(); i.hasNext();) {
            final JSONObject testCase = (JSONObject) i.next();
            final JSONObject schema = testCase.getJSONObject("schema");
            final List<String> errorMessages = testCase
                    .getJSONArray("errors")
                    .toList()
                    .stream()
                    .map(Object::toString)
                    .collect(toList());

            assertThat(checker.checkConstraints(schema).stream().map(Object::toString).collect(toList()),
                    is(errorMessages));
        }
    }

    @Test
    public void checksCompatibilityBetweenTwoOldAndNew() {

    }

//    @Test
//    public void whenNotAttributeUsedThenIncompatible() {
//
//    }
//
//    @Test
//    public void whenPatternPropertiesUsedThenIncompatible() {
//
//    }
//
//    @Test
//    public void whenTypeUndefinedThenIncompatible() {
//
//    }
//
//    @Test
//    public void whenArrayOfTypesThenIncompatible() {
//
//    }
//
//    @Test
//    public void whenNoChangesThenCompatible() {
//        final SchemaCompatibilityChecker checker = new SchemaCompatibilityChecker();
//        final JSONObject oldSchema = new JSONObject();
//        final JSONObject newSchema = new JSONObject();
//    }
//
//    @Test
//    public void whenAddOptionalFieldThenCompatible() {
//
//    }
//
//    @Test
//    public void whenNewOptionalFieldIsIncompatibleThenIncompatible() {
//
//    }
//
//    @Test
//    public void whenAddRequiredFieldThenIncompatible() {
//
//    }
//
//    @Test
//    public void whenAddDefinitionsThenCompatible() {
//
//    }
//
//    @Test
//    public void whenAddedDefinitionIsIncompatibleThenIncompatible() {
//
//    }
}
