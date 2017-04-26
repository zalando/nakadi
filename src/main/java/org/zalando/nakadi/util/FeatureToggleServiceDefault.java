package org.zalando.nakadi.util;

import com.google.common.collect.ImmutableSet;

import javax.ws.rs.NotSupportedException;
import java.util.Set;

/**
 * All features (real features) are enabled,
 * all deprecations (limitation) are not enabled.
 */
public class FeatureToggleServiceDefault implements FeatureToggleService {
    private static final Set<Feature> DEPRECATED_FEATURES = ImmutableSet.of(
            Feature.DISABLE_EVENT_TYPE_CREATION,
            Feature.DISABLE_SUBSCRIPTION_CREATION
    );

    @Override
    public void setFeature(final FeatureWrapper feature) {
        throw new NotSupportedException();
    }

    @Override
    public boolean isFeatureEnabled(final Feature feature) {
        return !DEPRECATED_FEATURES.contains(feature);
    }
}
