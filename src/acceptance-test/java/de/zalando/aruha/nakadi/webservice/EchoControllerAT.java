package de.zalando.aruha.nakadi.webservice;

import org.apache.http.HttpStatus;
import org.junit.Test;

import static com.jayway.restassured.RestAssured.given;
import static org.hamcrest.CoreMatchers.is;

public class EchoControllerAT extends BaseAT {

    private static final String ECHO_RESOURCE = "/api/echo";

    @Test
    public void echoServiceShouldEchoInput() {

        final String echo = "helloooo";
        given().param("toEcho", echo).when().get(ECHO_RESOURCE).then().statusCode(HttpStatus.SC_OK).body(is(echo));
    }
}
