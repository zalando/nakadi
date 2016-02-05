package de.zalando.aruha.nakadi.webservice.utils;

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

    private static final ObjectMapper jsonMapper = new ObjectMapper();

    private JsonTestHelper() {
    }

    public static List<Map<String, String>> asMapsList(final String body) throws IOException {
        return jsonMapper.<List<Map<String, String>>>readValue(body, LIST_OF_MAPS_REF);
    }

    public static Map<String, String> asMap(final String body) throws IOException {
        return jsonMapper.<Map<String, String>>readValue(body, MAP_REF);
    }
}
