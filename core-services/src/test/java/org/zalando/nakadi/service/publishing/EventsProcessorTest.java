package org.zalando.nakadi.service.publishing;

import org.json.JSONObject;
import org.junit.Test;
import org.mockito.Mockito;
import org.zalando.nakadi.utils.TestUtils;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;

public class EventsProcessorTest {

    private final EventPublisher eventPublisher = Mockito.mock(EventPublisher.class);

    @Test
    public void shouldSendEventWhenSubmitted() throws InterruptedException {
        final EventsProcessor eventsProcessor = new JsonEventProcessor(eventPublisher, 100, 1, 1, 10, 10);
        eventsProcessor.start();
        try {
            final JSONObject event = new JSONObject().put("path", "/path/to/event").put("user", "adyachkov");

            eventsProcessor.queueEvent("test_et_name", event);
            TestUtils.waitFor(() -> {
                try {
                    Mockito.verify(eventPublisher).processInternal(any(), any(), eq(false), eq(false));
                } catch (final Exception e) {
                    throw new AssertionError(e);
                }
            }, 500);
        } finally {
            eventsProcessor.stop();
        }
    }

}
