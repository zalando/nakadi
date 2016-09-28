package org.zalando.nakadi.util;

import org.springframework.stereotype.Service;

import java.util.LinkedList;
import java.util.List;

@Service
public interface FeatureToggleService {

    void setFeature(final FeatureWrapper feature);

    boolean isFeatureEnabled(final Feature feature);

    default List<FeatureWrapper> getFeatures() {
        final List<FeatureWrapper> features = new LinkedList<>();
        for (FeatureToggleService.Feature feature : FeatureToggleService.Feature.values()) {
            features.add(new FeatureWrapper(feature, isFeatureEnabled(feature)));
        }
        return features;
    }

    enum Feature {

        CONNECTION_CLOSE_CRUTCH("close_crutch"),
        DISABLE_EVENT_TYPE_CREATION("disable_event_type_creation"),
        DISABLE_EVENT_TYPE_DELETION("disable_event_type_deletion"),
        HIGH_LEVEL_API("high_level_api"),
        CHECK_APPLICATION_LEVEL_PERMISSIONS("check_application_level_permissions"),
        CHECK_PARTITIONS_KEYS("check_partitions_keys"),
        CHECK_OWNING_APPLICATION("check_owning_application");

        private final String id;

        Feature(final String id) {
            this.id = id;
        }

        public String getId() {
            return id;
        }
    }

    class FeatureWrapper {
        private Feature feature;
        private boolean enabled;

        public FeatureWrapper() {
        }

        public FeatureWrapper(final Feature feature, boolean enabled) {
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
}
