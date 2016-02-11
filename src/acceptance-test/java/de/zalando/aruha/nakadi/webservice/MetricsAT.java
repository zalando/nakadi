package de.zalando.aruha.nakadi.webservice;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

import static com.jayway.restassured.RestAssured.get;
import static com.jayway.restassured.RestAssured.given;
import static com.jayway.restassured.path.json.JsonPath.from;

import org.apache.http.HttpStatus;

import org.junit.Ignore;
import org.junit.Test;

public class MetricsAT extends BaseAT {

    @Test(timeout = 5000)
    public void whenGetMetricsThenStructureIsOk() {
//J-
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
//J+
    }

    @Test(timeout = 10000)
    public void whenCallEndpointThenCountInMetricsIncreased() {

        final String body = get("/metrics").andReturn().body().asString();
        final int getTopicsCount = from(body).getInt("timers.get_partitions.count");

        get("/event-types/blah/partitions");

//J-
        given()
                .when()
                .get("/metrics")
                .then()
                .statusCode(HttpStatus.SC_OK)
                .and()
                .body("timers.get_partitions.count", equalTo(getTopicsCount + 1));
//J+
    }

}
