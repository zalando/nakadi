package org.zalando.nakadi.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.luben.zstd.ZstdInputStream;
import org.apache.http.HttpStatus;
import org.eclipse.jetty.server.Request;
import org.json.JSONArray;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.http.HttpHeaders;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.zalando.problem.Problem;
import org.zalando.problem.ProblemModule;
import org.zalando.problem.Status;

import javax.servlet.ServletException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class CompressionBodyRequestFilterTest {

    @Test
    public void testDecompressOneZstdEvent() throws Exception {
        final CompressionBodyRequestFilter.FilterServletRequestWrapper wrapper =
                new CompressionBodyRequestFilter.FilterServletRequestWrapper(
                        Mockito.mock(Request.class),
                        new ZstdInputStream(
                                CompressionBodyRequestFilterTest.class.getResourceAsStream("event.zstd"))
                );

        final String json = new String(wrapper.getInputStream().readAllBytes());
        final JSONArray array = new JSONArray(json);

        Assert.assertEquals(1, array.length());
    }

    @Test
    public void shouldRespondBadRequestIfContentIsNotGZIP() throws ServletException, IOException {
        final var request = new MockHttpServletRequest();
        request.addHeader(HttpHeaders.CONTENT_ENCODING, "gzip");
        request.setMethod("POST");
        request.setContent("that's test data is not GZIP".getBytes(StandardCharsets.UTF_8));

        final var response = new MockHttpServletResponse();

        final var objectMapper = new ObjectMapper();
        objectMapper.registerModule(new ProblemModule());

        new CompressionBodyRequestFilter(objectMapper)
                .doFilter(request, response, null);

        Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatus());
        Assert.assertEquals(
                objectMapper.writeValueAsString(Problem.valueOf(Status.BAD_REQUEST, "Not in GZIP format")),
                response.getContentAsString());
    }

}