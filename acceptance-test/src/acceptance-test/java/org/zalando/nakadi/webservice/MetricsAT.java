package org.zalando.nakadi.webservice;

import org.apache.http.HttpStatus;
import org.junit.Test;

import static com.jayway.restassured.RestAssured.given;
import static org.hamcrest.Matchers.notNullValue;

public class MetricsAT extends BaseAT {

    @Test(timeout = 10000)
    public void whenGetMetricsThenStructureIsOk() {
        given()
                .when()
                .get("/metrics")
                .then()
                .statusCode(HttpStatus.SC_OK)
                .and()
                .body("counters", notNullValue())
                .body("gauges", notNullValue())
                .body("histograms", notNullValue())
                .body("meters", notNullValue())
                .body("timers", notNullValue())
                .body("version", notNullValue());
    }

}
