package org.zalando.nakadi.service;

import org.json.JSONObject;
import org.junit.Test;
import org.mockito.Mockito;
import org.zalando.nakadi.util.UUIDGenerator;
import org.zalando.nakadi.utils.TestUtils;

public class EventsProcessorTest {

    private final EventPublisher eventPublisher = Mockito.mock(EventPublisher.class);
    private final UUIDGenerator uuidGenerator = Mockito.mock(UUIDGenerator.class);

    @Test
    public void shouldSendEventWhenSubmitted() {
        final EventsProcessor eventsProcessor = new EventsProcessor(eventPublisher, uuidGenerator, 100, 1, 1, 100, 10);
        final JSONObject event = new JSONObject().put("path", "/path/to/event").put("user", "adyachkov");

        eventsProcessor.enrichAndSubmit("test_et_name", event);
        TestUtils.waitFor(() -> {
            try {
                Mockito.verify(eventPublisher).publishInternal(Mockito.any(), Mockito.any(),
                        Mockito.eq(false), Mockito.any());
            } catch (final Exception e) {
                throw new AssertionError(e);
            }
        }, 500);
    }

}
