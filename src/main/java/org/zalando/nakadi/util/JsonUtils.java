package org.zalando.nakadi.util;

import org.zalando.nakadi.domain.StrictJsonParser;
import org.zalando.nakadi.exceptions.runtime.InvalidEventTypeException;

public class JsonUtils {


    public static void checkEventTypeSchemaValid(final String jsonInString) throws InvalidEventTypeException {
        try {
            StrictJsonParser.parse(jsonInString, false);
        } catch (final RuntimeException jpe) {
            throw new InvalidEventTypeException("schema must be a valid json: " + jpe.getMessage());
        }
    }
}
