package org.zalando.nakadi.plugin.auth;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.zalando.problem.Problem;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

public class ProblemWriter {
    private final ObjectMapper objectMapper;

    public ProblemWriter(final ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public void writeProblem(final HttpServletResponse response, final Problem problem) throws IOException {
        response.setStatus(problem.getStatus().getStatusCode());
        response.setContentType("application/problem+json");
        response.getWriter().write(objectMapper.writeValueAsString(problem));
    }

}
