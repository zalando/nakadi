package org.zalando.nakadi.exceptions.runtime;

import org.zalando.nakadi.domain.Feature;

public class FeatureNotAvailableException extends NakadiBaseException {
    private final Feature feature;

    public FeatureNotAvailableException(final String message, final Feature feature) {
        super(message);
        this.feature = feature;
    }

    public Feature getFeature() {
        return feature;
    }
}
