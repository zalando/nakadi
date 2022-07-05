package org.zalando.nakadi.webservice;

import org.apache.http.HttpStatus;
import org.junit.Test;
import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.domain.SubscriptionBase;
import org.zalando.nakadi.utils.RandomSubscriptionBuilder;
import org.zalando.nakadi.webservice.utils.TestStreamingClient;

import java.io.IOException;

import static com.jayway.restassured.RestAssured.given;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.zalando.nakadi.domain.SubscriptionBase.InitialPosition.END;
import static org.zalando.nakadi.utils.TestUtils.waitFor;
import static org.zalando.nakadi.webservice.utils.NakadiTestUtils.createEventType;
import static org.zalando.nakadi.webservice.utils.NakadiTestUtils.createSubscription;

public class AuditLogAT extends BaseAT {

    private static final String AUDIT_LOG_ET = "nakadi.audit.log";

    @Test
    public void testAuditLogEventTypeIsCreated() {
        given()
                .get("/event-types/" + AUDIT_LOG_ET)
                .then()
                .statusCode(HttpStatus.SC_OK)
                .body("name", equalTo("nakadi.audit.log"))
                .body("owning_application", equalTo("stups_nakadi"));
    }

    @Test(timeout = 10000L)
    public void testAuditLogEventIsSent() throws IOException, InterruptedException {
        // subscribe to audit log
        final SubscriptionBase subscriptionBase = RandomSubscriptionBuilder.builder()
                .withEventType(AUDIT_LOG_ET)
                .withStartFrom(END)
                .buildSubscriptionBase();
        final Subscription subscription = createSubscription(subscriptionBase);
        final TestStreamingClient client = new TestStreamingClient(URL, subscription.getId(), "batch_limit=1");
        client.start();

        // create event type
        createEventType();

        // expect event to be posted to audit log as a reaction to event type creation
        waitFor(() -> assertThat(client.getJsonBatches().size(), greaterThan(0)));
    }
}
