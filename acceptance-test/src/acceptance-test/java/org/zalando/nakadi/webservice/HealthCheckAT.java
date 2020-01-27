package org.zalando.nakadi.webservice;

import org.apache.http.HttpStatus;
import org.junit.Test;

import static com.jayway.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;

public class HealthCheckAT extends BaseAT {

    @Test(timeout = 5000)
    public void whenHealthCheckThenOk() {
        given().when().get("/health").then().statusCode(HttpStatus.SC_OK).body(equalTo("OK"));
    }

}
