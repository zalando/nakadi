package org.zalando.nakadi.webservice;

import org.junit.Assert;
import org.junit.Test;
import org.zalando.nakadi.utils.EventTypeTestBuilder;
import org.zalando.nakadi.webservice.utils.NakadiTestUtils;

import static com.jayway.restassured.RestAssured.given;
import static com.jayway.restassured.http.ContentType.JSON;

public class StorageControllerAT extends BaseAT {

    @Test
    public void shouldChangeDefaultStorageWhenRequested() throws Exception {
        final String defaultConfig = given()
                .accept(JSON)
                .get("/storages/default")
                .print();

        given()
                .body(defaultConfig.replace("default", "default-test"))
                .contentType(JSON).post("/storages");

        NakadiTestUtils.createEventTypeInNakadi(EventTypeTestBuilder.builder().name("event_a").build());
        String storageId = (String) NakadiTestUtils.listTimelines("event_a").get(0).get("storage_id");
        Assert.assertEquals("default", storageId);

        given().contentType(JSON).put("/storages/default/default-test");
        NakadiTestUtils.createEventTypeInNakadi(EventTypeTestBuilder.builder().name("event_b").build());
        storageId = (String) NakadiTestUtils.listTimelines("event_b").get(0).get("storage_id");
        Assert.assertEquals("default-test", storageId);

        // cleanup
        given().contentType(JSON).put("/storages/default/default");
    }
}
