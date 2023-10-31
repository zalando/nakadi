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
@ConfigurationProperties("nakadi.dlq")
public class NakadiDeadLetterQueueInitialization {
    private static final Logger LOG = LoggerFactory.getLogger(NakadiDeadLetterQueueInitialization.class);

    private final SystemEventTypeInitializer systemEventTypeInitializer;

    private String storeEventTypeName;
    private String owningApplication;
    private String authDataType;
    private String authValue;

    @Autowired
    public NakadiDeadLetterQueueInitialization(final SystemEventTypeInitializer systemEventTypeInitializer) {
        this.systemEventTypeInitializer = systemEventTypeInitializer;
    }

    @EventListener
    public void onApplicationEvent(final ContextRefreshedEvent event) throws IOException {
        final Map<String, String> replacements = new HashMap<>();
        replacements.put("store_event_type_name_placeholder", storeEventTypeName);
        replacements.put("owning_application_placeholder", owningApplication);
        replacements.put("auth_data_type_placeholder", authDataType);
        replacements.put("auth_value_placeholder", authValue);

        systemEventTypeInitializer.createEventTypesFromResource("dead_letter_queue_event_types.json", replacements);
    }

    public String getStoreEventTypeName() {
        return storeEventTypeName;
    }

    public void setStoreEventTypeName(final String storeEventTypeName) {
        this.storeEventTypeName = storeEventTypeName;
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
