package org.zalando.nakadi.job;

import com.google.common.collect.Lists;
import com.jayway.restassured.specification.RequestSpecification;
import org.junit.Test;
import org.zalando.nakadi.webservice.RealEnvironmentAT;

import static com.jayway.restassured.http.ContentType.JSON;
import static org.springframework.http.HttpStatus.OK;

public class NakadiKpiInitializationAT extends RealEnvironmentAT {
    @Test
    public void testKpiEventTypesCreatedOnApplicationLaunch() {
        Lists.newArrayList("nakadi.event.type.log", "nakadi.subscription.log", "nakadi.data.streamed",
                "nakadi.batch.published", "nakadi.access.log")
                .forEach(et -> jsonRequestSpec()
                        .get("/event-types/" + et)
                        .then()
                        .statusCode(OK.value()));
    }

    private RequestSpecification jsonRequestSpec() {
        return requestSpec()
                .header("accept", "application/json")
                .contentType(JSON);
    }
}
