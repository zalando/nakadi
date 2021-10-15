package org.zalando.nakadi.service.publishing;

import org.json.JSONObject;
import org.junit.Test;
import org.mockito.Mockito;
import org.zalando.nakadi.security.UsernameHasher;

import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.verify;

public class NakadiKpiPublisherTest {

    private final EventsProcessor eventsProcessor = Mockito.mock(EventsProcessor.class);
    private final UsernameHasher usernameHasher = new UsernameHasher("123");

    @Test
    public void testPublishWithFeatureToggleOn() throws Exception {
        final Supplier<JSONObject> dataSupplier = () -> null;
        new NakadiKpiPublisher(eventsProcessor, usernameHasher, new EventMetadataTestStub())
                .publish("test_et_name", dataSupplier);

        verify(eventsProcessor).queueEvent("test_et_name", dataSupplier.get());
    }

    @Test
    public void testHash() throws Exception {
        final NakadiKpiPublisher publisher = new NakadiKpiPublisher(eventsProcessor,
                usernameHasher, new EventMetadataTestStub());
        assertThat(publisher.hash("application"),
                equalTo("befee725ab2ed3b17020112089a693ad8d8cfbf62b2442dcb5b89d66ce72391e"));
    }

}
