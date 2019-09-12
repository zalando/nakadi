package org.zalando.nakadi.service;

import org.springframework.stereotype.Service;
import org.zalando.nakadi.exceptions.runtime.FeatureNotAvailableException;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@Service
public interface FeatureToggleService {

    void setFeature(FeatureWrapper feature);

    boolean isFeatureEnabled(Feature feature);

    void setAuditLogPublisher(NakadiAuditLogPublisher auditLogPublisher);

    default void checkFeatureOn(final Feature feature) {
        if (!isFeatureEnabled(feature)) {
            throw new FeatureNotAvailableException("Feature " + feature + " is disabled", feature);
        }
    }

    default List<FeatureWrapper> getFeatures() {
        return Arrays.stream(Feature.values())
                .map(feature -> new FeatureWrapper(feature, isFeatureEnabled(feature)))
                .collect(Collectors.toList());
    }

    enum Feature {

        CONNECTION_CLOSE_CRUTCH("close_crutch"),
        DISABLE_EVENT_TYPE_CREATION("disable_event_type_creation"),
        DISABLE_EVENT_TYPE_DELETION("disable_event_type_deletion"),
        DELETE_EVENT_TYPE_WITH_SUBSCRIPTIONS("delete_event_type_with_subscriptions"),
        EVENT_TYPE_DELETION_ONLY_ADMINS("event_type_deletion_only_admins"),
        DISABLE_SUBSCRIPTION_CREATION("disable_subscription_creation"),
        REMOTE_TOKENINFO("remote_tokeninfo"),
        KPI_COLLECTION("kpi_collection"),
        AUDIT_LOG_COLLECTION("audit_log_collection"),
        DISABLE_DB_WRITE_OPERATIONS("disable_db_write_operations"),
        DISABLE_LOG_COMPACTION("disable_log_compaction"),
        FORCE_EVENT_TYPE_AUTHZ("force_event_type_authz"),
        FORCE_SUBSCRIPTION_AUTHZ("force_subscription_authz");

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
