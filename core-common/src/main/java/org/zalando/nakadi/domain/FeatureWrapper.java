package org.zalando.nakadi.domain;

import javax.validation.constraints.NotNull;

public class FeatureWrapper {
    @NotNull
    private Feature feature;
    @NotNull
    private boolean enabled;

    public FeatureWrapper() {
    }

    public FeatureWrapper(final Feature feature, final boolean enabled) {
        this.feature = feature;
        this.enabled = enabled;
    }

    public Feature getFeature() {
        return feature;
    }

    public boolean isEnabled() {
        return enabled;
    }
}
