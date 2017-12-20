package org.zalando.nakadi.service;

import org.json.JSONObject;
import org.junit.Test;
import org.mockito.Mockito;
import org.zalando.nakadi.util.FeatureToggleService;

import java.util.function.Supplier;

public class NakadiKpiPublisherTest {

    private final FeatureToggleService featureToggleService = Mockito.mock(FeatureToggleService.class);
    private final EventsProcessor eventsProcessor = Mockito.mock(EventsProcessor.class);

    @Test
    public void testPublishWithFeatureToggleOn() throws Exception {
        Mockito.when(featureToggleService.isFeatureEnabled(FeatureToggleService.Feature.KPI_COLLECTION))
                .thenReturn(true);
        final Supplier<JSONObject> dataSupplier = () -> null;
        new NakadiKpiPublisher(featureToggleService, eventsProcessor)
                .publish("test_et_name", dataSupplier);

        Mockito.verify(eventsProcessor).enrichAndSubmit("test_et_name", dataSupplier.get());
    }

    @Test
    public void testPublishWithFeatureToggleOff() throws Exception {
        Mockito.when(featureToggleService.isFeatureEnabled(FeatureToggleService.Feature.KPI_COLLECTION))
                .thenReturn(false);
        final Supplier<JSONObject> dataSupplier = () -> null;
        new NakadiKpiPublisher(featureToggleService, eventsProcessor)
                .publish("test_et_name", dataSupplier);

        Mockito.verify(eventsProcessor, Mockito.never()).enrichAndSubmit("test_et_name", dataSupplier.get());
    }

}
