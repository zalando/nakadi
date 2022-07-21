package org.zalando.nakadi.domain;

public enum Feature {

    DISABLE_EVENT_TYPE_CREATION("disable_event_type_creation"),
    DISABLE_EVENT_TYPE_DELETION("disable_event_type_deletion"),
    DELETE_EVENT_TYPE_WITH_SUBSCRIPTIONS("delete_event_type_with_subscriptions"),
    EVENT_TYPE_DELETION_ONLY_ADMINS("event_type_deletion_only_admins"),
    DISABLE_SUBSCRIPTION_CREATION("disable_subscription_creation"),
    REMOTE_TOKENINFO("remote_tokeninfo"),
    KPI_COLLECTION("kpi_collection"),
    AUDIT_LOG_COLLECTION("audit_log_collection"),
    DISABLE_DB_WRITE_OPERATIONS("disable_db_write_operations"),
    FORCE_EVENT_TYPE_AUTHZ("force_event_type_authz"),
    FORCE_SUBSCRIPTION_AUTHZ("force_subscription_authz"),
    REPARTITIONING("repartitioning"),
    EVENT_OWNER_SELECTOR_AUTHZ("event_owner_selector_authz"),
    ACCESS_LOG_ENABLED("access_log_enabled"),
    TOKEN_SUBSCRIPTIONS_ITERATION("token_subscription_iteration"),
    RETURN_BODY_ON_CREATE_UPDATE_EVENT_TYPE("return_body_on_create_update_event_type"),
    VALIDATE_SUBSCRIPTION_OWNING_APPLICATION("validate_subscription_owning_app"),
    VALIDATE_EVENT_TYPE_OWNING_APPLICATION("validate_event_type_owning_app");

    private final String id;

    Feature(final String id) {
        this.id = id;
    }

    public String getId() {
        return id;
    }
}
