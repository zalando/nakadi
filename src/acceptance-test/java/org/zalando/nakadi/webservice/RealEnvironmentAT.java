package org.zalando.nakadi.webservice;

import com.jayway.restassured.RestAssured;
import com.jayway.restassured.specification.RequestSpecification;

import java.util.Optional;

import static com.jayway.restassured.RestAssured.given;
import static java.util.Optional.ofNullable;

public abstract class RealEnvironmentAT {

    protected final Optional<String> oauthToken;
    protected final String owningApp;

    public RealEnvironmentAT() {
        oauthToken = ofNullable(System.getenv("NAKADI_OAUTH_TOKEN"));
        owningApp = ofNullable(System.getenv("NAKADI_OWNING_APP")).orElse("dummy-app");

        RestAssured.baseURI = ofNullable(System.getenv("NAKADI_BASE_URL"))
                .orElse(RestAssured.DEFAULT_URI);

        RestAssured.port = Integer.parseInt(ofNullable(System.getenv("NAKADI_PORT"))
                .orElse(Integer.toString(RestAssured.DEFAULT_PORT)));
    }

    protected RequestSpecification requestSpec() {
        final RequestSpecification requestSpec = given();
        oauthToken.ifPresent(token -> requestSpec.header("Authorization", "Bearer " + token));
        return requestSpec;
    }

}
