package org.zalando.nakadi.service;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.zalando.nakadi.repository.db.EventTypeCache;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class EventTypeChangeListenerTest {

    private EventTypeChangeListener listener;
    private Consumer<String> notificationTrigger;

    @Before
    public void setUp() {
        final EventTypeCache cache = mock(EventTypeCache.class);
        final ArgumentCaptor<Consumer> changeCall = ArgumentCaptor.forClass(Consumer.class);

        listener = new EventTypeChangeListener(cache);
        verify(cache, times(1)).addInvalidationListener(changeCall.capture());

        notificationTrigger = changeCall.getValue();
    }

    @Test
    public void testPossibleToCallUnknownET() {
        notificationTrigger.accept("any");
    }

    @Test
    public void testNotificationTriggered() throws IOException {
        final AtomicInteger triggerCount = new AtomicInteger();
        final String eventType = "test";
        try (Closeable subscription = listener.registerListener(
                (et) -> triggerCount.incrementAndGet(), Collections.singletonList(eventType))) {
            for (int i = 0; i < 10; ++i) {
                Assert.assertEquals(i, triggerCount.get());
                notificationTrigger.accept(eventType);
                Assert.assertEquals(i + 1, triggerCount.get());
                notificationTrigger.accept(eventType + "_fake");
                Assert.assertEquals(i + 1, triggerCount.get());
            }
        }
    }

    @Test
    public void testNotificationNotTriggeredAfterClosing() throws IOException {
        final AtomicInteger triggerCount = new AtomicInteger();
        final String eventType = "test";
        try (Closeable subscription = listener.registerListener(
                (et) -> triggerCount.incrementAndGet(), Collections.singletonList(eventType))) {
            notificationTrigger.accept(eventType);
        }
        Assert.assertEquals(1, triggerCount.get());
        notificationTrigger.accept(eventType);
        Assert.assertEquals(1, triggerCount.get());
    }

    @Test
    public void testSeveralListenersAreCalled() throws IOException {
        final AtomicInteger triggerCount1 = new AtomicInteger();
        final AtomicInteger triggerCount2 = new AtomicInteger();
        final String eventType = "test";
        try (Closeable subscription1 = listener.registerListener(
                (et) -> triggerCount1.incrementAndGet(), Collections.singletonList(eventType))) {
            try (Closeable subscription2 = listener.registerListener(
                    (et) -> triggerCount2.incrementAndGet(), Collections.singletonList(eventType))) {
                for (int i = 0; i < 10; ++i) {
                    Assert.assertEquals(i, triggerCount1.get());
                    Assert.assertEquals(i, triggerCount2.get());
                    notificationTrigger.accept(eventType);
                    Assert.assertEquals(i + 1, triggerCount1.get());
                    Assert.assertEquals(i + 1, triggerCount2.get());
                }
            }
        }
    }
}