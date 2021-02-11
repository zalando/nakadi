package org.zalando.nakadi.webservice;

import org.apache.http.HttpStatus;
import org.junit.Test;

import static com.jayway.restassured.RestAssured.given;

public class InvalidRequestAT {
    @Test(timeout = 10000)
    public void whenRequestRejectedExceptionThrownThenResponseIs400() {
        given()
                .when()
                .get("//")
                .then()
                .statusCode(HttpStatus.SC_BAD_REQUEST);
    }
}
