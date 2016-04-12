package de.zalando.aruha.nakadi.webservice;

import static com.jayway.restassured.RestAssured.given;
import org.apache.http.HttpStatus;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import org.junit.Test;

public class VersionControllerAT extends BaseAT {
    private static final String VERSION_RESOURCE = "/version";

    @Test(timeout = 10000)
    public void scmSourceShouldContainCorrectData() {
        given().when().get(VERSION_RESOURCE).then()
                .assertThat().statusCode(HttpStatus.SC_OK)
                .and().assertThat().body("scm_source.revision", allOf(notNullValue(), not("")))
                .and().assertThat().body("scm_source.url", allOf(notNullValue(), not("")))
                .and().assertThat().body("scm_source.author", allOf(notNullValue(), not("")))
                .and().assertThat().body("scm_source.status", notNullValue());
    }
}
