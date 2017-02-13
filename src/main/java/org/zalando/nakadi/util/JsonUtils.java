package org.zalando.nakadi.util;

import com.grack.nanojson.JsonParser;
import com.grack.nanojson.JsonParserException;
import org.zalando.nakadi.exceptions.InvalidEventTypeException;

public class JsonUtils {


    public static void checkEventTypeSchemaValid(final String jsonInString) throws InvalidEventTypeException {
        try {
            JsonParser.any().from(jsonInString);
        } catch (final JsonParserException jpe) {
            throw new InvalidEventTypeException("schema must be a valid json: " + jpe.getMessage());
        }
    }
}
