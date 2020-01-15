package org.zalando.nakadi.domain;

public class FeatureWrapper {
    private Feature feature;
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
