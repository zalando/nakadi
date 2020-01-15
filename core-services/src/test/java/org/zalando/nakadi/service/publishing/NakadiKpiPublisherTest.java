package org.zalando.nakadi.service.publishing;

import org.json.JSONObject;
import org.junit.Test;
import org.mockito.Mockito;
import org.zalando.nakadi.domain.Feature;
import org.zalando.nakadi.security.UsernameHasher;
import org.zalando.nakadi.service.FeatureToggleService;

import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class NakadiKpiPublisherTest {

    private final FeatureToggleService featureToggleService = Mockito.mock(FeatureToggleService.class);
    private final EventsProcessor eventsProcessor = Mockito.mock(EventsProcessor.class);
    private final UsernameHasher usernameHasher = new UsernameHasher("123");

    @Test
    public void testPublishWithFeatureToggleOn() throws Exception {
        when(featureToggleService.isFeatureEnabled(Feature.KPI_COLLECTION))
                .thenReturn(true);
        final Supplier<JSONObject> dataSupplier = () -> null;
        new NakadiKpiPublisher(featureToggleService, eventsProcessor, usernameHasher)
                .publish("test_et_name", dataSupplier);

        verify(eventsProcessor).enrichAndSubmit("test_et_name", dataSupplier.get());
    }

    @Test
    public void testPublishWithFeatureToggleOff() throws Exception {
        when(featureToggleService.isFeatureEnabled(Feature.KPI_COLLECTION))
                .thenReturn(false);
        final Supplier<JSONObject> dataSupplier = () -> null;
        new NakadiKpiPublisher(featureToggleService, eventsProcessor, usernameHasher)
                .publish("test_et_name", dataSupplier);

        verify(eventsProcessor, Mockito.never()).enrichAndSubmit("test_et_name", dataSupplier.get());
    }

    @Test
    public void testHash() throws Exception {
        final NakadiKpiPublisher publisher = new NakadiKpiPublisher(featureToggleService, eventsProcessor,
                usernameHasher);
        assertThat(publisher.hash("application"),
                equalTo("befee725ab2ed3b17020112089a693ad8d8cfbf62b2442dcb5b89d66ce72391e"));
    }

}
