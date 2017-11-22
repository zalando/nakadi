package org.zalando.nakadi.service.kpi;

import org.json.JSONObject;
import org.zalando.nakadi.util.FeatureToggleService;

public abstract class AbstractNakadiPublisher {

    private final FeatureToggleService featureToggleService;
    private final NakadiPublisher publisher;

    protected AbstractNakadiPublisher(final FeatureToggleService featureToggleService,
                                      final NakadiPublisher publisher) {
        this.featureToggleService = featureToggleService;
        this.publisher = publisher;
    }

    protected void publish(final String etName, final Object... data) {
        if (!featureToggleService.isFeatureEnabled(FeatureToggleService.Feature.KPI_COLLECTION)) {
            return;
        }

        final JSONObject event = prepareData(data);
        publisher.enrichAndSubmit(event, etName);
    }

    protected abstract JSONObject prepareData(final Object[] data);

}
