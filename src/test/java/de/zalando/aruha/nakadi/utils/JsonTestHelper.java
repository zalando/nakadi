package de.zalando.aruha.nakadi.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.zalando.problem.Problem;
import uk.co.datumedge.hamcrest.json.SameJSONAs;

import static uk.co.datumedge.hamcrest.json.SameJSONAs.sameJSONAs;

public class JsonTestHelper {

    private ObjectMapper objectMapper;

    public JsonTestHelper(final ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public SameJSONAs<? super String> matchesProblem(final Problem expectedProblem)
            throws JsonProcessingException {
        return sameJSONAs(asJsonString(expectedProblem));
    }

    public String asJsonString(final Problem expectedProblem) throws JsonProcessingException {
        return objectMapper.writeValueAsString(expectedProblem);
    }
}
