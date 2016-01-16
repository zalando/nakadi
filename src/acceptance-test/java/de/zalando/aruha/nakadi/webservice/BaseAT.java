package de.zalando.aruha.nakadi.webservice;

import com.jayway.restassured.RestAssured;
import com.jayway.restassured.parsing.Parser;

public abstract class BaseAT {

  protected static final int PORT = 8080;
  protected static final String URL = "http://localhost:" + PORT;

  static {
    RestAssured.port = PORT;
    RestAssured.defaultParser = Parser.JSON;
  }
}
