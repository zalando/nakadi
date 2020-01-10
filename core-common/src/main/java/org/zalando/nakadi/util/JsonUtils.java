package org.zalando.nakadi.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.joda.time.DateTime;
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

    public static String serializeDateTime(final ObjectMapper objectMapper, final DateTime dateTime) {
        try {
            final String serialized = objectMapper.writeValueAsString(dateTime);
            return serialized.substring(1, serialized.length() - 1);
        } catch (final JsonProcessingException e) {
            throw new RuntimeException("Failed to serizalize date " + dateTime + " to json", e);
        }
    }

}
