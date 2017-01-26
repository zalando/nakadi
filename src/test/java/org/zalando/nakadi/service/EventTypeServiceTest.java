package org.zalando.nakadi.service;

import org.junit.Test;
import org.zalando.nakadi.exceptions.InvalidEventTypeException;

public class EventTypeServiceTest {

    private final EventTypeService eventTypeService =
            new EventTypeService(null, null, null, null, null, null, null, null);

    @Test(expected = InvalidEventTypeException.class)
    public void testValidateSchemaEndingBracket() throws InvalidEventTypeException {
        eventTypeService.checkJsonIsValid("{\"additionalProperties\": true}}");
    }

    @Test(expected = InvalidEventTypeException.class)
    public void testValidateSchemaMultipleRoots() throws InvalidEventTypeException {
        eventTypeService.checkJsonIsValid("{\"additionalProperties\": true}{\"additionalProperties\": true}");
    }

    @Test(expected = InvalidEventTypeException.class)
    public void testValidateSchemaArbitraryEnding() throws InvalidEventTypeException {
        eventTypeService.checkJsonIsValid("{\"additionalProperties\": true}NakadiRocks");
    }

    @Test(expected = InvalidEventTypeException.class)
    public void testValidateSchemaArrayEnding() throws InvalidEventTypeException {
        eventTypeService.checkJsonIsValid("[{\"additionalProperties\": true}]]");
    }

    @Test(expected = InvalidEventTypeException.class)
    public void testValidateSchemaEndingCommaArray() throws InvalidEventTypeException {
        eventTypeService.checkJsonIsValid("[{\"test\": true},]");
    }

    @Test(expected = InvalidEventTypeException.class)
    public void testValidateSchemaEndingCommaArray2() throws InvalidEventTypeException {
        eventTypeService.checkJsonIsValid("[\"test\",]");
    }

    @Test(expected = InvalidEventTypeException.class)
    public void testValidateSchemaEndingCommaObject() throws InvalidEventTypeException {
        eventTypeService.checkJsonIsValid("{\"test\": true,}");
    }

    @Test
    public void testValidateSchemaFormattedJson() throws InvalidEventTypeException {
        eventTypeService.checkJsonIsValid("{\"properties\":{\"event_class\":{\"type\":\"string\"},\"app_domain_id\":" +
                "{\"type\":\"integer\"},\"event_type\":{\"type\":\"string\"},\"time\":{\"type\":\"number\"}," +
                "\"partitioning_key\":{\"type\":\"string\"},\"body\":{\"type\":\"object\"}}," +
                "\"additionalProperties\":true}");
    }

}