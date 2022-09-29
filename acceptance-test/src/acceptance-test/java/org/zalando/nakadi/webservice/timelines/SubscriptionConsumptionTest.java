package org.zalando.nakadi.webservice.timelines;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.domain.SubscriptionBase;
import org.zalando.nakadi.utils.RandomSubscriptionBuilder;
import org.zalando.nakadi.view.SubscriptionCursorWithoutToken;
import org.zalando.nakadi.webservice.BaseAT;
import org.zalando.nakadi.webservice.hila.StreamBatch;
import org.zalando.nakadi.webservice.utils.TestStreamingClient;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.zalando.nakadi.webservice.utils.NakadiTestUtils.createEventType;
import static org.zalando.nakadi.webservice.utils.NakadiTestUtils.createSubscription;
import static org.zalando.nakadi.webservice.utils.NakadiTestUtils.createTimeline;
import static org.zalando.nakadi.webservice.utils.NakadiTestUtils.publishEvents;

public class SubscriptionConsumptionTest {

    private static EventType eventType;
    private static Subscription subscription;
    private static String[] cursorsDuringPublish;

    @BeforeClass
    public static void setupEventTypeWithEvents() throws IOException, InterruptedException {
        eventType = createEventType();
        subscription = createSubscription(
                RandomSubscriptionBuilder.builder().withEventType(eventType.getName()).build());
        final CountDownLatch finished = new CountDownLatch(1);
        final AtomicReference<String[]> inTimeCursors = new AtomicReference<>();
        createParallelConsumer(subscription, 8, finished, inTimeCursors::set);

        publishEvents(eventType.getName(), 2, i -> "{\"foo\":\"bar\"}");
        createTimeline(eventType.getName());
        publishEvents(eventType.getName(), 2, i -> "{\"foo\":\"bar\"}");
        createTimeline(eventType.getName());
        publishEvents(eventType.getName(), 2, i -> "{\"foo\":\"bar\"}");
        createTimeline(eventType.getName());
        publishEvents(eventType.getName(), 2, i -> "{\"foo\":\"bar\"}");

        finished.await();
        cursorsDuringPublish = inTimeCursors.get();
    }

    @Test(timeout = 65000)
    public void test2TimelinesInaRow() throws IOException, InterruptedException {
        final EventType eventType = createEventType();
        final Subscription subscription = createSubscription(
                RandomSubscriptionBuilder.builder().withEventType(eventType.getName()).build());
        final CountDownLatch finished = new CountDownLatch(1);
        final AtomicReference<String[]> inTimelineCursors = new AtomicReference<>();
        createParallelConsumer(subscription, 5, finished, inTimelineCursors::set);
        publishEvents(eventType.getName(), 2, i -> "{\"foo\":\"bar\"}");
        createTimeline(eventType.getName()); // Still old topic
        createTimeline(eventType.getName()); // New topic
        createTimeline(eventType.getName()); // Another new topic
        publishEvents(eventType.getName(), 1, i ->"{\"foo\":\"bar\"}");
        createTimeline(eventType.getName());
        createTimeline(eventType.getName());
        publishEvents(eventType.getName(), 2, i -> "{\"foo\":\"bar\"}");
        finished.await();
        Assert.assertArrayEquals(
                new String[]{
                        "001-0001-000000000000000000",
                        "001-0001-000000000000000001",
                        "001-0004-000000000000000000",
                        "001-0006-000000000000000000",
                        "001-0006-000000000000000001"
                },
                inTimelineCursors.get()
        );

        // Will create subscription clone
        final Subscription clone = createSubscription(
                RandomSubscriptionBuilder.builder().withEventType(eventType.getName())
                        .withStartFrom(SubscriptionBase.InitialPosition.BEGIN).build());
        final CountDownLatch finished2 = new CountDownLatch(1);
        createParallelConsumer(clone, 5, finished2, inTimelineCursors::set);
        finished2.await();
        Assert.assertArrayEquals(
                new String[]{
                        "001-0001-000000000000000000",
                        "001-0001-000000000000000001",
                        "001-0004-000000000000000000",
                        "001-0006-000000000000000000",
                        "001-0006-000000000000000001"
                },
                inTimelineCursors.get()
        );
    }

    @Test
    public void test2TimelinesInaRowNoBegin() throws IOException, InterruptedException {
        final EventType eventType = createEventType();
        final Subscription subscription = createSubscription(
                RandomSubscriptionBuilder.builder().withEventType(eventType.getName()).build());

        final CountDownLatch finished = new CountDownLatch(1);
        final AtomicReference<String[]> inTimelineCursors = new AtomicReference<>();
        createParallelConsumer(subscription, 2, finished, inTimelineCursors::set);
        createTimeline(eventType.getName()); // Still old topic
        createTimeline(eventType.getName()); // New topic
        createTimeline(eventType.getName()); // Another new topic
        publishEvents(eventType.getName(), 2, i -> "{\"foo\":\"bar\"}");
        finished.await();
        Assert.assertArrayEquals(
                new String[]{
                        "001-0004-000000000000000000",
                        "001-0004-000000000000000001",
                },
                inTimelineCursors.get()
        );

        final Subscription subscription2 = createSubscription(RandomSubscriptionBuilder.builder()
                .withEventType(eventType.getName()).withStartFrom(SubscriptionBase.InitialPosition.BEGIN).build());

        final CountDownLatch finished2 = new CountDownLatch(1);
        final AtomicReference<String[]> inTimelineCursors2 = new AtomicReference<>();
        createParallelConsumer(subscription2, 2, finished2, inTimelineCursors2::set);
        finished2.await();
        Assert.assertArrayEquals(
                new String[]{
                        "001-0004-000000000000000000",
                        "001-0004-000000000000000001",
                },
                inTimelineCursors2.get()
        );
    }

    @Test
    public void testInTimeCursorsCorrect() {
        Assert.assertArrayEquals(
                new String[]{
                        "001-0001-000000000000000000",
                        "001-0001-000000000000000001",
                        "001-0002-000000000000000000",
                        "001-0002-000000000000000001",
                        "001-0003-000000000000000000",
                        "001-0003-000000000000000001",
                        "001-0004-000000000000000000",
                        "001-0004-000000000000000001"

                },
                cursorsDuringPublish
        );
    }

    @Test
    public void testAllEventsConsumed() throws IOException, InterruptedException {
        final String[] expected = new String[]{
                "001-0001-000000000000000000",
                "001-0001-000000000000000001",
                "001-0002-000000000000000000",
                "001-0002-000000000000000001",
                "001-0003-000000000000000000",
                "001-0003-000000000000000001",
                "001-0004-000000000000000000",
                "001-0004-000000000000000001"
        };

        // Do not test last case, because it makes no sense...
        for (int idx = -1; idx < expected.length - 1; ++idx) {
            final CountDownLatch finished = new CountDownLatch(1);
            final AtomicReference<String[]> receivedOffset = new AtomicReference<>();
            final Subscription subscription = createSubscription(
                    RandomSubscriptionBuilder.builder().withEventType(eventType.getName())
                            .withStartFrom(SubscriptionBase.InitialPosition.CURSORS)
                            .withInitialCursors(Collections.singletonList(
                                    new SubscriptionCursorWithoutToken(
                                            eventType.getName(),
                                            "0",
                                            idx == -1 ? "BEGIN" : expected[idx]))).build());
            createParallelConsumer(subscription, expected.length - 1 - idx, finished, receivedOffset::set);
            finished.await();

            final String[] testedOffsets = Arrays.copyOfRange(expected, idx + 1, expected.length);
            Assert.assertArrayEquals(testedOffsets, receivedOffset.get());
        }
    }


    private static void createParallelConsumer(
            final Subscription subscription,
            final int expectedEvents,
            final CountDownLatch finished,
            final Consumer<String[]> inTimeCursors) throws InterruptedException {
        final String params = Stream.of(
                "batch_limit=1",
                "batch_flush_timeout=1",
                "stream_limit=" + expectedEvents,
                "stream_timeout=30").collect(Collectors.joining("&"));
        final TestStreamingClient streamingClient = new TestStreamingClient(BaseAT.URL, subscription.getId(), params);
        streamingClient.startWithAutocommit(batches -> {
            inTimeCursors.accept(batchesToCursors(batches));
            finished.countDown();
        });
    }

    private static String[] batchesToCursors(final List<StreamBatch> batches) {
        return batches.stream()
                .filter(sb -> null != sb.getEvents())
                .filter(sb -> !sb.getEvents().isEmpty())
                .map(sb -> sb.getCursor().getOffset())
                .toArray(String[]::new);
    }


}
