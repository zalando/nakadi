package org.zalando.nakadi.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.domain.EventTypeBase;
import org.zalando.nakadi.exceptions.runtime.DuplicatedEventTypeNameException;
import org.zalando.nakadi.exceptions.runtime.NakadiBaseException;

import java.io.IOException;
import java.util.Optional;

@Component
@ConfigurationProperties(prefix = "nakadi.audit")
public class NakadiAuditLogInitialization {
    private static final Logger LOG = LoggerFactory.getLogger(NakadiAuditLogInitialization.class);

    private final ObjectMapper objectMapper;
    private final EventTypeService eventTypeService;
    private final FeatureToggleService featureToggleService;

    private String eventType;
    private String owningApplication;
    private String authorization;

    @Autowired
    public NakadiAuditLogInitialization(final ObjectMapper objectMapper, final EventTypeService eventTypeService,
                                        final FeatureToggleService featureToggleService) {
        this.objectMapper = objectMapper;
        this.eventTypeService = eventTypeService;
        this.featureToggleService = featureToggleService;
    }

    @EventListener
    public void onApplicationEvent(final ContextRefreshedEvent event) throws IOException {
        if (!featureToggleService.isFeatureEnabled(FeatureToggleService.Feature.AUDIT_LOG_COLLECTION)) {
            LOG.debug("Audit log collection is disabled, skip creation of audit log event type");
            return;
        }

        LOG.debug("Initializing Audit log event type");

        String auditEventTypeString = Resources
                .toString(Resources.getResource("audit_event_type.json"), Charsets.UTF_8);

        auditEventTypeString = auditEventTypeString.replaceAll("event_type_name_placeholder", eventType);
        auditEventTypeString = auditEventTypeString.replaceAll("owning_application_placeholder", owningApplication);
        auditEventTypeString = auditEventTypeString.replaceAll("authorization_placeholder", authorization);

        final TypeReference<EventTypeBase> typeReference = new TypeReference<EventTypeBase>() {
        };
        final EventTypeBase eventType = objectMapper.readValue(auditEventTypeString, typeReference);

        try {
            eventTypeService.create(eventType, Optional.of(owningApplication));
        } catch (final DuplicatedEventTypeNameException e) {
            LOG.debug("Audit event type already exists " + eventType.getName());
        } catch (final NakadiBaseException e) {
            LOG.debug("Problem creating audit event type " + eventType.getName(), e);
        }
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

    public String getAuthorization() {
        return authorization;
    }

    public void setAuthorization(final String authorization) {
        this.authorization = authorization;
    }
}
