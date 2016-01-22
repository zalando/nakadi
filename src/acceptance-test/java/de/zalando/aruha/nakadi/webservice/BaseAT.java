package de.zalando.aruha.nakadi.webservice;

import com.jayway.restassured.RestAssured;
import com.jayway.restassured.parsing.Parser;

public abstract class BaseAT {

    protected static final int PORT = 8080;
    protected static final String URL = "http://localhost:" + PORT;

    protected static final String TOPIC = "test-topic";
    protected static final int PARTITION_NUM = 8;

    static {
        RestAssured.port = PORT;
        RestAssured.defaultParser = Parser.JSON;
    }
}
