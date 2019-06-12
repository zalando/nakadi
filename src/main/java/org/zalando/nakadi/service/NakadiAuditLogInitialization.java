package org.zalando.nakadi.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@Component
@ConfigurationProperties(prefix = "nakadi.audit")
public class NakadiAuditLogInitialization {
    private static final Logger LOG = LoggerFactory.getLogger(NakadiAuditLogInitialization.class);

    private final SystemEventTypeInitializer systemEventTypeInitializer;
    private final FeatureToggleService featureToggleService;

    private String eventType;
    private String owningApplication;
    private String authDataType;
    private String authValue;

    @Autowired
    public NakadiAuditLogInitialization(
            final SystemEventTypeInitializer systemEventTypeInitializer,
            final FeatureToggleService featureToggleService) {
        this.systemEventTypeInitializer = systemEventTypeInitializer;
        this.featureToggleService = featureToggleService;
    }

    @EventListener
    public void onApplicationEvent(final ContextRefreshedEvent event) throws IOException {
        if (!featureToggleService.isFeatureEnabled(FeatureToggleService.Feature.AUDIT_LOG_COLLECTION)) {
            LOG.debug("Audit log collection is disabled, skip creation of audit log event type");
            return;
        }
        final Map<String, String> replacements = new HashMap<>();
        replacements.put("event_type_name_placeholder", eventType);
        replacements.put("owning_application_placeholder", owningApplication);
        replacements.put("auth_data_type_placeholder", authDataType);
        replacements.put("auth_value_placeholder", authValue);

        systemEventTypeInitializer.createEventTypesFromResource("audit_event_types.json", replacements);
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(final String eventType) {
        this.eventType = eventType;
    }

    public String getOwningApplication() {
        return owningApplication;
    }

    public void setOwningApplication(final String owningApplication) {
        this.owningApplication = owningApplication;
    }

    public String getAuthDataType() {
        return authDataType;
    }

    public void setAuthDataType(final String authDataType) {
        this.authDataType = authDataType;
    }

    public String getAuthValue() {
        return authValue;
    }

    public void setAuthValue(final String authValue) {
        this.authValue = authValue;
    }
}
