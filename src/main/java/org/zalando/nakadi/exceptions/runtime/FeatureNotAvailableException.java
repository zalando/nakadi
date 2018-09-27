package org.zalando.nakadi.exceptions.runtime;

import org.zalando.nakadi.service.FeatureToggleService;

public class FeatureNotAvailableException extends NakadiBaseException {
    private final FeatureToggleService.Feature feature;

    public FeatureNotAvailableException(final String message, final FeatureToggleService.Feature feature) {
        super(message);
        this.feature = feature;
    }

    public FeatureToggleService.Feature getFeature() {
        return feature;
    }
}
