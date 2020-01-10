package org.zalando.nakadi.util;

import org.junit.Test;
import org.zalando.nakadi.exceptions.runtime.InvalidEventTypeException;

public class JsonUtilsTest {

    @Test(expected = InvalidEventTypeException.class)
    public void testValidateSchemaEndingBracket() throws InvalidEventTypeException {
        JsonUtils.checkEventTypeSchemaValid("{\"additionalProperties\": true}}");
    }

    @Test(expected = InvalidEventTypeException.class)
    public void testValidateSchemaMultipleRoots() throws InvalidEventTypeException {
        JsonUtils.checkEventTypeSchemaValid("{\"additionalProperties\": true}{\"additionalProperties\": true}");
    }

    @Test(expected = InvalidEventTypeException.class)
    public void testValidateSchemaArbitraryEnding() throws InvalidEventTypeException {
        JsonUtils.checkEventTypeSchemaValid("{\"additionalProperties\": true}NakadiRocks");
    }

    @Test(expected = InvalidEventTypeException.class)
    public void testValidateSchemaArrayEnding() throws InvalidEventTypeException {
        JsonUtils.checkEventTypeSchemaValid("[{\"additionalProperties\": true}]]");
    }

    @Test(expected = InvalidEventTypeException.class)
    public void testValidateSchemaEndingCommaArray() throws InvalidEventTypeException {
        JsonUtils.checkEventTypeSchemaValid("[{\"test\": true},]");
    }

    @Test(expected = InvalidEventTypeException.class)
    public void testValidateSchemaEndingCommaArray2() throws InvalidEventTypeException {
        JsonUtils.checkEventTypeSchemaValid("[\"test\",]");
    }

    @Test(expected = InvalidEventTypeException.class)
    public void testValidateSchemaEndingCommaObject() throws InvalidEventTypeException {
        JsonUtils.checkEventTypeSchemaValid("{\"test\": true,}");
    }

    @Test
    public void testValidateSchemaFormattedJson() throws InvalidEventTypeException {
        JsonUtils.checkEventTypeSchemaValid("{\"properties\":{\"event_class\":{\"type\":\"string\"}," +
                "\"app_domain_id\":{\"type\":\"integer\"},\"event_type\":{\"type\":\"string\"},\"time\"" +
                ":{\"type\":\"number\"},\"partitioning_key\":{\"type\":\"string\"},\"body\":{\"type\"" +
                ":\"object\"}},\"additionalProperties\":true}");
    }
}