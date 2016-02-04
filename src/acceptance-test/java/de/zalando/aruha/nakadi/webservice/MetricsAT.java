package de.zalando.aruha.nakadi.webservice;

import org.apache.http.HttpStatus;
import org.junit.Ignore;
import org.junit.Test;

import static com.jayway.restassured.RestAssured.get;
import static com.jayway.restassured.RestAssured.given;
import static com.jayway.restassured.path.json.JsonPath.from;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class MetricsAT extends BaseAT {

    @Test(timeout = 5000)
    public void whenGetMetricsThenStructureIsOk() {
        given().when().get("/metrics").then().statusCode(HttpStatus.SC_OK).and().body("counters", notNullValue())
               .body("gauges", notNullValue()).body("histograms", notNullValue()).body("meters", notNullValue())
               .body("timers", notNullValue()).body("version", notNullValue());
    }

    @Ignore("Ignore until the new partion resource in the /event-types endpoint is available")
    @Test(timeout = 10000)
    public void whenCallEndpointThenCountInMetricsIncreased() {

        final String body = get("/metrics").andReturn().body().asString();
        final int getTopicsCount = from(body).getInt("timers.get_topics.count");

        get("/topics");

        given().when().get("/metrics").then().statusCode(HttpStatus.SC_OK).and().body("timers.get_topics.count",
            equalTo(getTopicsCount + 1));
    }

}
