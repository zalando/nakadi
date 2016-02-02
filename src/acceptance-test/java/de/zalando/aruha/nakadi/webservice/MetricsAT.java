package de.zalando.aruha.nakadi.webservice;

import org.apache.http.HttpStatus;
import org.junit.Test;

import static com.jayway.restassured.RestAssured.get;
import static com.jayway.restassured.RestAssured.given;
import static com.jayway.restassured.path.json.JsonPath.from;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class MetricsAT extends BaseAT {

    @Test(timeout = 5000)
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

    @Test(timeout = 10000)
    public void whenCallEndpointThenCountInMetricsIncreased() {

        final String body = get("/metrics").andReturn().body().asString();
        final int getPartitionsCount = from(body).getInt("timers.get_partitions.count");

        get("/event-types/blah/partitions");

        given()
                .when()
                .get("/metrics")
                .then()
                .statusCode(HttpStatus.SC_OK)
                .and()
                .body("timers.get_partitions.count", equalTo(getPartitionsCount + 1));
    }

}
