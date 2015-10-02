package de.zalando.bazaar.lab.webservice;

import static org.hamcrest.CoreMatchers.is;

import static com.jayway.restassured.RestAssured.given;
import static com.jayway.restassured.RestAssured.when;

import org.apache.http.HttpStatus;

import org.junit.Before;
import org.junit.Test;

import org.junit.runner.RunWith;

import org.springframework.boot.test.SpringApplicationConfiguration;

import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;

import com.jayway.restassured.RestAssured;

import de.zalando.bazaar.lab.Application;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = Application.class)
@WebAppConfiguration
public class EchoControllerAT {

    private static final String ECHO_RESOURCE = "/api/echo";

    @Before
    public void setUp() {
        RestAssured.port = 8080;
    }

    @Test
    public void echoServiceShouldEchoInput() {

        final String echo = "helloooo";
        given().param("toEcho", echo).when().get(ECHO_RESOURCE).then().statusCode(HttpStatus.SC_OK).body(is(echo));
    }
}
