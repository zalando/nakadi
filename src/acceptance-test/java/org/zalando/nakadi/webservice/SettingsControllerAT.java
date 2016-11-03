package org.zalando.nakadi.webservice;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.jayway.restassured.http.ContentType;
import org.apache.curator.framework.CuratorFramework;
import org.apache.http.HttpStatus;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.zalando.nakadi.config.JsonConfig;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.service.BlacklistService;
import org.zalando.nakadi.utils.JsonTestHelper;
import org.zalando.nakadi.webservice.utils.NakadiTestUtils;
import org.zalando.nakadi.webservice.utils.ZookeeperTestUtils;

import java.io.IOException;
import java.util.Collections;

import static com.jayway.restassured.RestAssured.given;

public class SettingsControllerAT extends BaseAT {

    private static final String FLOODERS_URL = "/settings/flooders";
    private static final ObjectMapper MAPPER = (new JsonConfig()).jacksonObjectMapper();
    private static final JsonTestHelper JSON_HELPER = new JsonTestHelper(MAPPER);
    private static final CuratorFramework CURATOR = ZookeeperTestUtils.createCurator(ZOOKEEPER_URL);

    @After
    public void setUp() {
        clearFloodersData();
    }

    @Test
    public void testBlockFlooder() throws Exception {
        final EventType eventType = NakadiTestUtils.createEventType();
        blockFlooder(new BlacklistService.Flooder(eventType.getName(), BlacklistService.Type.CONSUMER_ET));

        Assert.assertNotNull(CURATOR.checkExists()
                .forPath("/nakadi/flooders/consumers/event_types/" + eventType.getName()));
    }

    @Test
    public void testUnblockFlooder() throws Exception {
        final EventType eventType = NakadiTestUtils.createEventType();
        final BlacklistService.Flooder flooder =
                new BlacklistService.Flooder(eventType.getName(), BlacklistService.Type.CONSUMER_ET);
        blockFlooder(flooder);

        unblockFlooder(flooder);

        Assert.assertNull(CURATOR.checkExists()
                .forPath("/nakadi/flooders/consumers/event_types/" + eventType.getName()));
    }

    @Test
    public void testGetFlooders() throws Exception {
        final EventType eventType = NakadiTestUtils.createEventType();

        final BlacklistService.Flooder flooder =
                new BlacklistService.Flooder(eventType.getName(), BlacklistService.Type.CONSUMER_ET);
        blockFlooder(flooder);

        given()
                .contentType(ContentType.JSON)
                .get(FLOODERS_URL)
                .then()
                .statusCode(HttpStatus.SC_OK)
                .content(JSON_HELPER.matchesObject(
                        ImmutableMap.of(
                                "consumers",  ImmutableMap.of(
                                        "event_types", Collections.singleton(eventType.getName()),
                                        "apps", Collections.emptySet()),
                                "producers", ImmutableMap.of(
                                        "event_types", Collections.emptySet(),
                                        "apps", Collections.emptySet()))
                        )
                );
    }

    private void clearFloodersData() {
        try {
            CURATOR.delete().deletingChildrenIfNeeded().forPath("/nakadi/flooders/consumers");
            CURATOR.delete().deletingChildrenIfNeeded().forPath("/nakadi/flooders/producers");
        } catch (final Exception exception) {
            // nothing to do
        }
    }

    public static void blockFlooder(final BlacklistService.Flooder flooder) throws IOException {
        given()
                .body(MAPPER.writeValueAsString(flooder))
                .contentType(ContentType.JSON)
                .post(FLOODERS_URL)
                .then()
                .statusCode(HttpStatus.SC_NO_CONTENT);
    }

    public static void unblockFlooder(final BlacklistService.Flooder flooder) throws JsonProcessingException {
        given()
                .body(MAPPER.writeValueAsString(flooder))
                .contentType(ContentType.JSON)
                .delete(FLOODERS_URL)
                .then()
                .statusCode(HttpStatus.SC_NO_CONTENT);
    }

}