package org.zalando.nakadi.webservice;

import com.jayway.restassured.RestAssured;
import com.jayway.restassured.specification.RequestSpecification;

import java.util.Optional;
import org.zalando.nakadi.config.Configuration;

import static java.util.Optional.ofNullable;

public abstract class RealEnvironmentAT {

    protected final Optional<String> oauthToken;
    protected final String owningApp;

    public RealEnvironmentAT() {
        oauthToken = ofNullable(System.getenv("NAKADI_OAUTH_TOKEN"));
        owningApp = ofNullable(System.getenv("NAKADI_OWNING_APP")).orElse("dummy-app");

        // Get configurations from automation.yml file
        Configuration configs = BaseAT.configs;

        RestAssured.baseURI = ofNullable(configs.getApiUrl())
                .orElse(RestAssured.DEFAULT_URI);

        RestAssured.port = Optional.of(configs.getApiPort())
                .orElse(RestAssured.DEFAULT_PORT);
    }

    protected RequestSpecification requestSpec() {
        final RequestSpecification requestSpec = RestAssured.given();
        oauthToken.ifPresent(token -> requestSpec.header("Authorization", "Bearer " + token));
        return requestSpec;
    }

}
