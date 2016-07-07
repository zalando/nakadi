package de.zalando.aruha.nakadi.webservice;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.zalando.aruha.nakadi.config.JsonConfig;
import de.zalando.aruha.nakadi.domain.EventType;
import de.zalando.aruha.nakadi.utils.JsonTestHelper;
import org.apache.http.HttpStatus;
import org.json.JSONObject;
import org.junit.Test;
import org.zalando.problem.Problem;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPOutputStream;

import static com.jayway.restassured.RestAssured.given;
import static com.jayway.restassured.http.ContentType.JSON;
import static de.zalando.aruha.nakadi.utils.TestUtils.buildEventType;
import static de.zalando.aruha.nakadi.utils.TestUtils.randomValidEventTypeName;
import static java.text.MessageFormat.format;
import static javax.ws.rs.core.HttpHeaders.CONTENT_ENCODING;
import static javax.ws.rs.core.Response.Status.NOT_ACCEPTABLE;

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
                .post(format("/event-types/{0}/events", eventType.getName()))
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
        final EventType eventType = buildEventType(randomValidEventTypeName(),
                new JSONObject("{\"type\": \"object\", \"properties\": {\"blah\": {\"type\": \"string\"}}, " +
                        "\"required\": [\"blah\"]}"));
        given()
                .body(MAPPER.writeValueAsString(eventType))
                .contentType(JSON)
                .post("/event-types");
        return eventType;
    }

    private byte[] compressWithGzip(final String string) throws IOException {
        final ByteArrayOutputStream baOutputStream = new ByteArrayOutputStream(string.length());
        final GZIPOutputStream gzipOutputStream = new GZIPOutputStream(baOutputStream);
        gzipOutputStream.write(string.getBytes());
        gzipOutputStream.close();
        final byte[] compressed = baOutputStream.toByteArray();
        baOutputStream.close();
        return compressed;
    }

}
