package org.zalando.nakadi.webservice.utils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JsonTestHelper {

    private static final TypeReference<ArrayList<HashMap<String, String>>> LIST_OF_MAPS_REF =
            new TypeReference<ArrayList<HashMap<String, String>>>() {};

    private static final TypeReference<HashMap<String, String>> MAP_REF =
            new TypeReference<HashMap<String, String>>() {};

    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    private JsonTestHelper() {
    }

    public static List<Map<String, String>> asMapsList(final String body) throws IOException {
        final TypeReference<List<Map<String, String>>> typeReference = new TypeReference<List<Map<String, String>>>() {
        };
        return JSON_MAPPER.readValue(body, typeReference);
    }

    public static Map<String, String> asMap(final String body) throws IOException {
        return JSON_MAPPER.readValue(body, MAP_REF);
    }
}
