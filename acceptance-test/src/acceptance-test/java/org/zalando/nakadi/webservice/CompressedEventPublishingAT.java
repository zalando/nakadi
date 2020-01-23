package org.zalando.nakadi.webservice;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import org.apache.http.HttpStatus;
import org.json.JSONObject;
import org.junit.Test;
import org.zalando.nakadi.config.JsonConfig;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.utils.EventTypeTestBuilder;
import org.zalando.nakadi.utils.JsonTestHelper;
import org.zalando.problem.Problem;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPOutputStream;

import static com.jayway.restassured.RestAssured.given;
import static com.jayway.restassured.http.ContentType.JSON;
import static org.springframework.http.HttpHeaders.CONTENT_ENCODING;
import static org.zalando.problem.Status.NOT_ACCEPTABLE;

public class CompressedEventPublishingAT extends BaseAT {

    private static final ObjectMapper MAPPER = (new JsonConfig()).jacksonObjectMapper();
    private static final JsonTestHelper JSON_HELPER = new JsonTestHelper(MAPPER);

    @Test
    public void whenSubmitCompressedBodyWithGzipEncodingThenOk() throws IOException {

        final EventType eventType = createEventTypeWithSchema();
        final byte[] bodyCompressed = compressWithGzip("[{\"blah\":\"bloh\"}]");

        given()
                .body(bodyCompressed)
                .contentType(JSON)
                .header(CONTENT_ENCODING, "gzip")
                .post("/event-types/{et}/events", eventType.getName())
                .then()
                .statusCode(HttpStatus.SC_OK);
    }

    @Test
    public void whenGetWithGzipEncodingThenNotAcceptable() throws IOException {

        final Problem expectedProblem = Problem.valueOf(NOT_ACCEPTABLE,
                "GET method doesn't support gzip content encoding");
        given()
                .header(CONTENT_ENCODING, "gzip")
                .get("/event-types")
                .then()
                .body(JSON_HELPER.matchesObject(expectedProblem))
                .statusCode(HttpStatus.SC_NOT_ACCEPTABLE);
    }

    private EventType createEventTypeWithSchema() throws JsonProcessingException {
        final EventType eventType = EventTypeTestBuilder.builder()
                .schema(new JSONObject("{\"type\": \"object\", \"properties\": {\"blah\": {\"type\": \"string\"}}, " +
                        "\"required\": [\"blah\"]}")).build();
        given()
                .body(MAPPER.writeValueAsString(eventType))
                .contentType(JSON)
                .post("/event-types")
                .then()
                .statusCode(HttpStatus.SC_CREATED);
        return eventType;
    }

    private byte[] compressWithGzip(final String string) throws IOException {
        final ByteArrayOutputStream baOutputStream = new ByteArrayOutputStream(string.length());
        final GZIPOutputStream gzipOutputStream = new GZIPOutputStream(baOutputStream);
        gzipOutputStream.write(string.getBytes(Charsets.UTF_8));
        gzipOutputStream.close();
        final byte[] compressed = baOutputStream.toByteArray();
        baOutputStream.close();
        return compressed;
    }

}
