package org.zalando.nakadi.util;

import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@Service
public interface FeatureToggleService {

    void setFeature(FeatureWrapper feature);

    boolean isFeatureEnabled(Feature feature);

    default List<FeatureWrapper> getFeatures() {
        return Arrays.stream(Feature.values())
                .map(feature -> new FeatureWrapper(feature, isFeatureEnabled(feature)))
                .collect(Collectors.toList());
    }

    enum Feature {

        CONNECTION_CLOSE_CRUTCH("close_crutch"),
        DISABLE_EVENT_TYPE_CREATION("disable_event_type_creation"),
        DISABLE_EVENT_TYPE_DELETION("disable_event_type_deletion"),
        DISABLE_SUBSCRIPTION_CREATION("disable_subscription_creation"),
        HIGH_LEVEL_API("high_level_api"),
        CHECK_APPLICATION_LEVEL_PERMISSIONS("check_application_level_permissions"),
        CHECK_PARTITIONS_KEYS("check_partitions_keys"),
        CHECK_OWNING_APPLICATION("check_owning_application"),
        LIMIT_CONSUMERS_NUMBER("limit_consumers_number"),
        ZERO_PADDED_OFFSETS("zero_padded_offsets"),
        SEND_BATCH_VIA_OUTPUT_STREAM("send_batch_via_output_stream");

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
}
