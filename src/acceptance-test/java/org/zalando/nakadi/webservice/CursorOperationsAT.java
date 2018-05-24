package org.zalando.nakadi.webservice;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.jayway.restassured.RestAssured;
import com.jayway.restassured.http.ContentType;
import org.junit.Before;
import org.junit.Test;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.utils.EventTypeTestBuilder;
import org.zalando.nakadi.webservice.utils.NakadiTestUtils;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.springframework.http.HttpStatus.OK;
import static org.springframework.http.HttpStatus.UNPROCESSABLE_ENTITY;
import static org.zalando.nakadi.utils.TestUtils.randomTextString;
import static org.zalando.nakadi.webservice.utils.NakadiTestUtils.postEvents;

public class CursorOperationsAT {
    private EventType eventType;
    private static final String EVENT = "{\"foo\":\"" + randomTextString() + "\"}";

    @Before
    public void setUp() throws JsonProcessingException {
        eventType = EventTypeTestBuilder.builder().build();
        NakadiTestUtils.createEventTypeInNakadi(eventType);
    }

    @Test
    public void calculateCursorLag() {
        RestAssured.given()
                .contentType(ContentType.JSON)
                .body("[{\"partition\": \"0\", \"offset\":\"BEGIN\"}]")
                .when()
                .post("/event-types/" + eventType.getName() + "/cursors-lag")
                .then()
                .statusCode(OK.value())
                .body("size()", equalTo(1))
                .body("newest_available_offset[0]", equalTo("001-0001--1"))
                .body("oldest_available_offset[0]", equalTo("001-0001-000000000000000000"))
                .body("unconsumed_events[0]", equalTo(0));

        postEvents(eventType.getName(), EVENT, EVENT);

        RestAssured.given()
                .contentType(ContentType.JSON)
                .body("[{\"partition\": \"0\", \"offset\":\"BEGIN\"}]")
                .when()
                .post("/event-types/" + eventType.getName() + "/cursors-lag")
                .then()
                .statusCode(OK.value())
                .body("size()", equalTo(1))
                .body("newest_available_offset[0]", equalTo("001-0001-000000000000000001"))
                .body("oldest_available_offset[0]", equalTo("001-0001-000000000000000000"))
                .body("unconsumed_events[0]", equalTo(2));

        NakadiTestUtils.switchTimelineDefaultStorage(eventType);

        RestAssured.given()
                .contentType(ContentType.JSON)
                .body("[{\"partition\": \"0\", \"offset\":\"BEGIN\"}]")
                .when()
                .post("/event-types/" + eventType.getName() + "/cursors-lag")
                .then()
                .statusCode(OK.value())
                .body("size()", equalTo(1))
                .body("newest_available_offset[0]", equalTo("001-0001-000000000000000001"))
                .body("oldest_available_offset[0]", equalTo("001-0001-000000000000000000"))
                .body("unconsumed_events[0]", equalTo(2));

        NakadiTestUtils.switchTimelineDefaultStorage(eventType);

        RestAssured.given()
                .contentType(ContentType.JSON)
                .body("[{\"partition\": \"0\", \"offset\":\"BEGIN\"}]")
                .when()
                .post("/event-types/" + eventType.getName() + "/cursors-lag")
                .then()
                .statusCode(OK.value())
                .body("size()", equalTo(1))
                .body("newest_available_offset[0]", equalTo("001-0001-000000000000000001"))
                .body("oldest_available_offset[0]", equalTo("001-0001-000000000000000000"))
                .body("unconsumed_events[0]", equalTo(2));

        postEvents(eventType.getName(), EVENT, EVENT);

        RestAssured.given()
                .contentType(ContentType.JSON)
                .body("[{\"partition\": \"0\", \"offset\":\"BEGIN\"}]")
                .when()
                .post("/event-types/" + eventType.getName() + "/cursors-lag")
                .then()
                .statusCode(OK.value())
                .body("size()", equalTo(1))
                .body("newest_available_offset[0]", equalTo("001-0003-000000000000000001"))
                .body("oldest_available_offset[0]", equalTo("001-0001-000000000000000000"))
                .body("unconsumed_events[0]", equalTo(4));

        NakadiTestUtils.switchTimelineDefaultStorage(eventType);

        RestAssured.given()
                .contentType(ContentType.JSON)
                .body("[{\"partition\": \"0\", \"offset\":\"BEGIN\"}]")
                .when()
                .post("/event-types/" + eventType.getName() + "/cursors-lag")
                .then()
                .statusCode(OK.value())
                .body("size()", equalTo(1))
                .body("newest_available_offset[0]", equalTo("001-0003-000000000000000001"))
                .body("oldest_available_offset[0]", equalTo("001-0001-000000000000000000"))
                .body("unconsumed_events[0]", equalTo(4));

        postEvents(eventType.getName(), EVENT, EVENT, EVENT);

        RestAssured.given()
                .contentType(ContentType.JSON)
                .body("[{\"partition\": \"0\", \"offset\":\"BEGIN\"}]")
                .when()
                .post("/event-types/" + eventType.getName() + "/cursors-lag")
                .then()
                .statusCode(OK.value())
                .body("size()", equalTo(1))
                .body("newest_available_offset[0]", equalTo("001-0004-000000000000000002"))
                .body("oldest_available_offset[0]", equalTo("001-0001-000000000000000000"))
                .body("unconsumed_events[0]", equalTo(7));

        RestAssured.given()
                .contentType(ContentType.JSON)
                .body("[{\"partition\": \"0\", \"offset\":\"001-0001-000000000000000000\"}]")
                .when()
                .post("/event-types/" + eventType.getName() + "/cursors-lag")
                .then()
                .statusCode(OK.value())
                .body("size()", equalTo(1))
                .body("newest_available_offset[0]", equalTo("001-0004-000000000000000002"))
                .body("oldest_available_offset[0]", equalTo("001-0001-000000000000000000"))
                .body("unconsumed_events[0]", equalTo(6));
    }

    @Test
    public void unshiftCursor() {
        RestAssured.given()
                .contentType(ContentType.JSON)
                .body("[{\"partition\": \"0\", \"offset\":\"BEGIN\"}]")
                .when()
                .post("/event-types/" + eventType.getName() + "/shifted-cursors")
                .then()
                .statusCode(OK.value())
                .body("size()", equalTo(1))
                .body("offset[0]", equalTo("001-0001--1"))
                .body("partition[0]", equalTo("0"));

        NakadiTestUtils.switchTimelineDefaultStorage(eventType);

        RestAssured.given()
                .contentType(ContentType.JSON)
                .body("[{\"partition\": \"0\", \"offset\":\"BEGIN\"}]")
                .when()
                .post("/event-types/" + eventType.getName() + "/shifted-cursors")
                .then()
                .statusCode(OK.value())
                .body("size()", equalTo(1))
                .body("offset[0]", equalTo("001-0001--1"))
                .body("partition[0]", equalTo("0"));

        postEvents(eventType.getName(), EVENT, EVENT);

        RestAssured.given()
                .contentType(ContentType.JSON)
                .body("[{\"partition\": \"0\", \"offset\":\"BEGIN\", \"shift\":-1}]")
                .when()
                .post("/event-types/" + eventType.getName() + "/shifted-cursors")
                .then()
                .statusCode(UNPROCESSABLE_ENTITY.value());

        RestAssured.given()
                .contentType(ContentType.JSON)
                .body("[{\"partition\": \"0\", \"offset\":\"BEGIN\", \"shift\":1}]")
                .when()
                .post("/event-types/" + eventType.getName() + "/shifted-cursors")
                .then()
                .statusCode(OK.value())
                .body("size()", equalTo(1))
                .body("offset[0]", equalTo("001-0002-000000000000000000"))
                .body("partition[0]", equalTo("0"));

        RestAssured.given()
                .contentType(ContentType.JSON)
                .body("[{\"partition\": \"0\", \"offset\":\"BEGIN\", \"shift\":2}]")
                .when()
                .post("/event-types/" + eventType.getName() + "/shifted-cursors")
                .then()
                .statusCode(OK.value())
                .body("size()", equalTo(1))
                .body("offset[0]", equalTo("001-0002-000000000000000001"))
                .body("partition[0]", equalTo("0"));

        RestAssured.given()
                .contentType(ContentType.JSON)
                .body("[{\"partition\": \"0\", \"offset\":\"BEGIN\", \"shift\":3}]")
                .when()
                .post("/event-types/" + eventType.getName() + "/shifted-cursors")
                .then()
                .statusCode(OK.value())
                .body("size()", equalTo(1))
                .body("offset[0]", equalTo("001-0002-000000000000000002"))
                .body("partition[0]", equalTo("0"));

        NakadiTestUtils.switchTimelineDefaultStorage(eventType);

        postEvents(eventType.getName(), EVENT, EVENT);

        RestAssured.given()
                .contentType(ContentType.JSON)
                .body("[{\"partition\": \"0\", \"offset\":\"BEGIN\", \"shift\":3}]")
                .when()
                .post("/event-types/" + eventType.getName() + "/shifted-cursors")
                .then()
                .statusCode(OK.value())
                .body("size()", equalTo(1))
                .body("offset[0]", equalTo("001-0003-000000000000000000"))
                .body("partition[0]", equalTo("0"));

        RestAssured.given()
                .contentType(ContentType.JSON)
                .body("[{\"partition\": \"0\", \"offset\":\"BEGIN\", \"shift\":4}]")
                .when()
                .post("/event-types/" + eventType.getName() + "/shifted-cursors")
                .then()
                .statusCode(OK.value())
                .body("size()", equalTo(1))
                .body("offset[0]", equalTo("001-0003-000000000000000001"))
                .body("partition[0]", equalTo("0"));

        RestAssured.given()
                .contentType(ContentType.JSON)
                .body("[{\"partition\": \"0\", \"offset\":\"001-0003-000000000000000001\", \"shift\":-1}]")
                .when()
                .post("/event-types/" + eventType.getName() + "/shifted-cursors")
                .then()
                .statusCode(OK.value())
                .body("size()", equalTo(1))
                .body("offset[0]", equalTo("001-0003-000000000000000000"))
                .body("partition[0]", equalTo("0"));

        RestAssured.given()
                .contentType(ContentType.JSON)
                .body("[{\"partition\": \"0\", \"offset\":\"001-0003-000000000000000001\", \"shift\":-2}]")
                .when()
                .post("/event-types/" + eventType.getName() + "/shifted-cursors")
                .then()
                .statusCode(OK.value())
                .body("size()", equalTo(1))
                .body("offset[0]", equalTo("001-0003--1"))
                .body("partition[0]", equalTo("0"));

        RestAssured.given()
                .contentType(ContentType.JSON)
                .body("[{\"partition\": \"0\", \"offset\":\"001-0003-000000000000000001\", \"shift\":-3}]")
                .when()
                .post("/event-types/" + eventType.getName() + "/shifted-cursors")
                .then()
                .statusCode(OK.value())
                .body("size()", equalTo(1))
                .body("offset[0]", equalTo("001-0002-000000000000000000"))
                .body("partition[0]", equalTo("0"));

        RestAssured.given()
                .contentType(ContentType.JSON)
                .body("[{\"partition\": \"0\", \"offset\":\"001-0003-000000000000000001\", \"shift\":-4}]")
                .when()
                .post("/event-types/" + eventType.getName() + "/shifted-cursors")
                .then()
                .statusCode(OK.value())
                .body("size()", equalTo(1))
                .body("offset[0]", equalTo("001-0002--1"))
                .body("partition[0]", equalTo("0"));
    }
}
