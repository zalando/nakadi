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
import org.zalando.nakadi.utils.TestUtils;
import org.zalando.nakadi.webservice.utils.NakadiTestUtils;
import org.zalando.nakadi.webservice.utils.ZookeeperTestUtils;
import uk.co.datumedge.hamcrest.json.SameJSONAs;

import java.io.IOException;
import java.util.Collections;

import static com.jayway.restassured.RestAssured.given;

public class SettingsControllerAT extends BaseAT {

    private static final String BLACKLIST_URL = "/settings/blacklist";
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
        blacklist(eventType.getName(), BlacklistService.Type.CONSUMER_ET);

        Assert.assertNotNull(CURATOR.checkExists()
                .forPath("/nakadi/blacklist/consumers/event_types/" + eventType.getName()));
    }

    @Test
    public void testUnblockFlooder() throws Exception {
        final EventType eventType = NakadiTestUtils.createEventType();
        blacklist(eventType.getName(), BlacklistService.Type.CONSUMER_ET);

        whitelist(eventType.getName(), BlacklistService.Type.CONSUMER_ET);

        Assert.assertNull(CURATOR.checkExists()
                .forPath("/nakadi/blacklist/consumers/event_types/" + eventType.getName()));
    }

    @Test
    public void testGetFlooders() throws Exception {
        final EventType eventType = NakadiTestUtils.createEventType();
        blacklist(eventType.getName(), BlacklistService.Type.CONSUMER_ET);

        TestUtils.waitFor(() -> given()
                .contentType(ContentType.JSON)
                .get(BLACKLIST_URL)
                .then()
                .statusCode(HttpStatus.SC_OK)
                .content(getFloodersMatcher(eventType)), 1000, 200);

    }

    private SameJSONAs<? super String> getFloodersMatcher(final EventType eventType) {
        try {
            return JSON_HELPER.matchesObject(
                    ImmutableMap.of(
                            "consumers",  ImmutableMap.of(
                                    "event_types", Collections.singleton(eventType.getName()),
                                    "apps", Collections.emptySet()),
                            "producers", ImmutableMap.of(
                                    "event_types", Collections.emptySet(),
                                    "apps", Collections.emptySet()))
                    );
        } catch (JsonProcessingException e) {
            throw new RuntimeException();
        }
    }

    private void clearFloodersData() {
        try {
            CURATOR.delete().deletingChildrenIfNeeded().forPath("/nakadi/blacklist/consumers");
            CURATOR.delete().deletingChildrenIfNeeded().forPath("/nakadi/blacklist/producers");
        } catch (final Exception exception) {
            // nothing to do
        }
    }

    public static void blacklist(final String name, final BlacklistService.Type type) throws IOException {
        given()
                .contentType(ContentType.JSON)
                .put(String.format("%s/%s/%s", BLACKLIST_URL, type, name))
                .then()
                .statusCode(HttpStatus.SC_NO_CONTENT);
    }

    public static void whitelist(final String name, final BlacklistService.Type type) throws JsonProcessingException {
        given()
                .contentType(ContentType.JSON)
                .delete(String.format("%s/%s/%s", BLACKLIST_URL, type, name))
                .then()
                .statusCode(HttpStatus.SC_NO_CONTENT);
    }

}