package org.zalando.nakadi.service;

import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.util.FeatureToggleService;

import java.util.function.Supplier;

@Component
public class NakadiKpiPublisher {

    private static final Logger LOG = LoggerFactory.getLogger(NakadiKpiPublisher.class);

    private final FeatureToggleService featureToggleService;
    private final EventsProcessor eventsProcessor;

    @Autowired
    protected NakadiKpiPublisher(final FeatureToggleService featureToggleService,
                                 final EventsProcessor eventsProcessor) {
        this.featureToggleService = featureToggleService;
        this.eventsProcessor = eventsProcessor;
    }

    public void publish(final String etName, final Supplier<JSONObject> eventSupplier) {
        try {
            if (!featureToggleService.isFeatureEnabled(FeatureToggleService.Feature.KPI_COLLECTION)) {
                return;
            }

            final JSONObject event = eventSupplier.get();
            eventsProcessor.enrichAndSubmit(etName, event);
        } catch (final Exception e) {
            LOG.error("Error occurred when submitting KPI event for publishing: {}", e.getMessage());
        }
    }

}
