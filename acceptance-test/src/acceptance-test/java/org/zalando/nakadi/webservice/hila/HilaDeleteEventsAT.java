package org.zalando.nakadi.webservice.hila;

import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;
import org.zalando.nakadi.domain.CleanupPolicy;
import org.zalando.nakadi.domain.EnrichmentStrategyDescriptor;
import org.zalando.nakadi.domain.EventCategory;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.domain.SubscriptionBase;
import org.zalando.nakadi.partitioning.PartitionStrategy;
import org.zalando.nakadi.utils.EventTypeTestBuilder;
import org.zalando.nakadi.utils.RandomSubscriptionBuilder;
import org.zalando.nakadi.webservice.BaseAT;
import org.zalando.nakadi.webservice.utils.NakadiTestUtils;
import org.zalando.nakadi.webservice.utils.TestStreamingClient;

import java.io.IOException;
import java.util.Collections;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.zalando.nakadi.domain.SubscriptionBase.InitialPosition.BEGIN;
import static org.zalando.nakadi.utils.TestUtils.waitFor;
import static org.zalando.nakadi.webservice.utils.NakadiTestUtils.createEventTypeInNakadi;
import static org.zalando.nakadi.webservice.utils.NakadiTestUtils.createSubscription;
import static org.zalando.nakadi.webservice.utils.TestStreamingClient.SESSION_ID_UNKNOWN;

public class HilaDeleteEventsAT extends BaseAT {

    private Subscription subscription;
    private EventType eventType;

    @Before
    public void before() throws IOException {
        eventType = EventTypeTestBuilder.builder()
                .cleanupPolicy(CleanupPolicy.COMPACT)
                .partitionStrategy(PartitionStrategy.HASH_STRATEGY)
                .partitionKeyFields(Collections.singletonList("part_field"))
                .category(EventCategory.BUSINESS)
                .enrichmentStrategies(Lists.newArrayList(EnrichmentStrategyDescriptor.METADATA_ENRICHMENT))
                .schema("{\"type\": \"object\",\"properties\": " +
                        "{\"part_field\": {\"type\": \"string\"}},\"required\": [\"part_field\"]}")
                .build();

        createEventTypeInNakadi(eventType);

        final SubscriptionBase subscription = RandomSubscriptionBuilder.builder()
                .withEventType(eventType.getName())
                .withStartFrom(BEGIN)
                .buildSubscriptionBase();
        this.subscription = createSubscription(subscription);
    }

    private void publishEvent(final boolean delete, final String value) {
        final String event = "{\"metadata\":" +
                "{\"occurred_at\":\"2019-12-12T14:25:21Z\",\"eid\":\"f08976bc-9754-4eb2-a9b4-9c9c47d4ce64\"," +
                "\"partition_compaction_key\":\"" + value + "\"}, " +
                "\"part_field\": \"" + value + "\"}";
        if (delete) {
            NakadiTestUtils.deleteEvent(eventType.getName(), event);
        } else {
            NakadiTestUtils.publishEvent(eventType.getName(), event);
        }
    }

    @Test(timeout = 15000)
    public void testConsumptionForDeletedEvents() {
        final TestStreamingClient client = TestStreamingClient
                .create(URL, subscription.getId(),
                        "batch_flush_timeout=600&batch_limit=1&stream_timeout=2&stream_limit=6")
                .start();
        waitFor(() -> assertThat(client.getSessionId(), not(equalTo(SESSION_ID_UNKNOWN))));

        // upon execution, we expect that event type will receive:
        // 4 published events (1, 2, 3, 4)
        // 4 events deleted (3, 4, 5, 6)
        // 2 event published (5, 7)
        // After this operation consumer should receive exactly 6 events - 1, 2, 3, 4, 5, 7 every one of them only once.

        publishEvent(false, "1");
        publishEvent(false, "2");
        publishEvent(false, "3");
        publishEvent(false, "4");
        publishEvent(true, "3");
        publishEvent(true, "4");
        publishEvent(true, "5");
        publishEvent(true, "6");
        publishEvent(false, "5");
        publishEvent(false, "7");

        // when stream_timeout is reached we should get 7 batches:
        // 6 batches with events, 7-th with debug message
        waitFor(() -> assertThat(client.getJsonBatches(), hasSize(7)));
        assertThat(client.getJsonBatches().get(0).getEvents().get(0).get("part_field"), equalTo("1"));
        assertThat(client.getJsonBatches().get(1).getEvents().get(0).get("part_field"), equalTo("2"));
        assertThat(client.getJsonBatches().get(2).getEvents().get(0).get("part_field"), equalTo("3"));
        assertThat(client.getJsonBatches().get(3).getEvents().get(0).get("part_field"), equalTo("4"));
        assertThat(client.getJsonBatches().get(4).getEvents().get(0).get("part_field"), equalTo("5"));
        assertThat(client.getJsonBatches().get(5).getEvents().get(0).get("part_field"), equalTo("7"));
    }
}
