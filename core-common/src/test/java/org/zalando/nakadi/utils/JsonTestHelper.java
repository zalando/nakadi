package org.zalando.nakadi.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import uk.co.datumedge.hamcrest.json.SameJSONAs;

import static uk.co.datumedge.hamcrest.json.SameJSONAs.sameJSONAs;

public class JsonTestHelper {

    private ObjectMapper objectMapper;

    public JsonTestHelper(final ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public SameJSONAs<? super String> matchesObject(final Object expectedObject)
            throws JsonProcessingException {
        return sameJSONAs(asJsonString(expectedObject));
    }

    public String asJsonString(final Object object) throws JsonProcessingException {
        return objectMapper.writeValueAsString(object);
    }
}
