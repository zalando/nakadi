package org.zalando.nakadi.webservice;

import com.jayway.restassured.RestAssured;
import org.apache.http.HttpStatus;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.EventTypeSchema;
import org.zalando.nakadi.utils.EventTypeTestBuilder;
import org.zalando.nakadi.webservice.utils.NakadiTestUtils;

public class SchemaControllerAT extends BaseAT {

    @Test
    public void whenGetSchemasThenList() throws Exception {
        createEventType();
        RestAssured.given()
                .when()
                .get("/event-types/et_test_name/schemas")
                .then()
                .statusCode(HttpStatus.SC_OK)
                .and()
                .body("items.size()", Matchers.is(2))
                .body("items[0].version", Matchers.equalTo("1.1.0"))
                .body("items[1].version", Matchers.equalTo("1.0.0"))
                .body("_links.next", Matchers.nullValue())
                .body("_links.prev", Matchers.nullValue());
    }

    @Test
    public void whenGetSchemasThenListWithOffset() throws Exception {
        createEventType();
        RestAssured.given()
                .when()
                .get("/event-types/et_test_name/schemas?offset=0&limit=1")
                .then()
                .statusCode(HttpStatus.SC_OK)
                .and()
                .body("items.size()", Matchers.is(1))
                .body("items[0].version", Matchers.equalTo("1.1.0"))
                .body("_links.next.href", Matchers.equalTo("/event-types/et_test_name/schemas?offset=1&limit=1"))
                .body("_links.prev", Matchers.nullValue());

        RestAssured.given()
                .when()
                .get("/event-types/et_test_name/schemas?offset=1&limit=1")
                .then()
                .statusCode(HttpStatus.SC_OK)
                .and()
                .body("items.size()", Matchers.is(1))
                .body("items[0].version", Matchers.equalTo("1.0.0"))
                .body("_links.next", Matchers.nullValue())
                .body("_links.prev.href", Matchers.equalTo("/event-types/et_test_name/schemas?offset=0&limit=1"));
    }

    @Test
    public void whenGetSchemasThenListWithGreatOffset() throws Exception {
        createEventType();
        RestAssured.given()
                .when()
                .get("/event-types/et_test_name/schemas?offset=2&limit=5")
                .then()
                .statusCode(HttpStatus.SC_OK)
                .and()
                .body("items.size()", Matchers.is(0))
                .body("_links.next", Matchers.nullValue())
                .body("_links.prev", Matchers.nullValue());
        RestAssured.given()
                .when()
                .get("/event-types/et_test_name/schemas?offset=5&limit=2")
                .then()
                .statusCode(HttpStatus.SC_OK)
                .and()
                .body("items.size()", Matchers.is(0))
                .body("_links.next", Matchers.nullValue())
                .body("_links.prev", Matchers.nullValue());
    }

    @Test
    public void whenGetSchemasNoSchemasThen404() throws Exception {
        RestAssured.given()
                .when()
                .get("/event-types/XXX/schemas")
                .then()
                .statusCode(HttpStatus.SC_NOT_FOUND);
    }

    @Test
    public void whenCreateSchemaWithFetchThen200() throws Exception {
        createEventType();
        final EventTypeSchema expectedSchema = getEventType().getSchema();
        RestAssured.given()
                .when()
                .contentType("application/json")
                .body(getEventType().getSchema())
                .post("/event-types/et_test_name/schemas?fetch=true")
                .then()
                .statusCode(HttpStatus.SC_OK)
                .and()
                .body("schema", Matchers
                        .is(expectedSchema.getSchema()))
                .body("type", Matchers.is(expectedSchema.getType().
                        toString().toLowerCase()))
                .body("version", Matchers.notNullValue())
                .body("created_at", Matchers.notNullValue());
    }


    private void createEventType() throws Exception {
        EventType eventType =
                EventTypeTestBuilder.builder()
                        .schema("{ \"properties\": { \"order_number\": { \"type\": \"string\" }}}")
                        .name("et_test_name")
                        .build();
        NakadiTestUtils.createEventTypeInNakadi(eventType);
        eventType = getEventType();
        NakadiTestUtils.updateEventTypeInNakadi(eventType);
    }

    private EventType getEventType() {
        return EventTypeTestBuilder.builder()
                .schema("{ \"properties\": { \"order_number\": { \"type\": \"string\" }, " +
                        "\"order_number_2\": { \"type\": \"string\" }," +
                        "\"order_number_3\": { \"type\": \"string\" }}}")
                .name("et_test_name")
                .build();
    }

}