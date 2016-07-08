package de.zalando.aruha.nakadi.util;

public interface FeatureToggleService {
    boolean isFeatureEnabled(final Feature feature);

    enum Feature {

        CONNECTION_CLOSE_CRUTCH("close_crutch"),
        DISABLE_EVENT_TYPE_CREATION("disable_event_type_creation"),
        DISABLE_EVENT_TYPE_DELETION("disable_event_type_deletion"),
        HIGH_LEVEL_API("high_level_api");

        private final String id;

        Feature(final String id) {
            this.id = id;
        }

        public String getId() {
            return id;
        }
    }
}
