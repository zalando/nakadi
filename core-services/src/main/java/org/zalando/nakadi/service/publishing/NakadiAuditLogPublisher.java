package org.zalando.nakadi.service.publishing;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.config.SecuritySettings;
import org.zalando.nakadi.domain.Feature;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.plugin.api.authz.Subject;
import org.zalando.nakadi.security.UsernameHasher;
import org.zalando.nakadi.service.FeatureToggleService;

import java.util.Collections;
import java.util.Optional;

@Component
public class NakadiAuditLogPublisher {

    private static final Logger LOG = LoggerFactory.getLogger(NakadiAuditLogPublisher.class);

    private final FeatureToggleService featureToggleService;
    private final JsonEventProcessor eventsProcessor;
    private final UsernameHasher usernameHasher;
    private final String auditEventType;
    private final ObjectMapper objectMapper;
    private final AuthorizationService authorizationService;
    private final EventMetadata eventMetadata;

    @Autowired
    protected NakadiAuditLogPublisher(final FeatureToggleService featureToggleService,
                                      final JsonEventProcessor eventsProcessor,
                                      final EventMetadata eventMetadata,
                                      final ObjectMapper objectMapper,
                                      final UsernameHasher usernameHasher,
                                      final AuthorizationService authorizationService,
                                      @Value("${nakadi.audit.eventType}") final String auditEventType) {
        this.eventsProcessor = eventsProcessor;
        this.eventMetadata = eventMetadata;
        this.usernameHasher = usernameHasher;
        this.objectMapper = objectMapper;
        this.auditEventType = auditEventType;
        this.featureToggleService = featureToggleService;
        this.featureToggleService.setAuditLogPublisher(this);
        this.authorizationService = authorizationService;
    }

    public void publish(final Optional<Object> previousState,
                        final Optional<Object> newState,
                        final ResourceType resourceType,
                        final ActionType actionType,
                        final String resourceId) {
        try {
            if (!featureToggleService.isFeatureEnabled(Feature.AUDIT_LOG_COLLECTION)) {
                return;
            }

            final String user = authorizationService.getSubject().map(Subject::getName)
                    .orElse(SecuritySettings.UNAUTHENTICATED_CLIENT_ID);

            final Optional<String> previousEventText = previousState.map(this::serialize);
            final Optional<JSONObject> previousEventObject = previousEventText.map(JSONObject::new);

            final Optional<String> newEventText = newState.map(this::serialize);
            final Optional<JSONObject> newEventObject = newEventText.map(JSONObject::new);

            final JSONObject payload = new JSONObject()
                    .put("previous_object", previousEventObject.orElse(null))
                    .put("previous_text", previousEventText.orElse(null))
                    .put("new_object", newEventObject.orElse(null))
                    .put("new_text", newEventText.orElse(null))
                    .put("resource_type", resourceType.name().toLowerCase())
                    .put("resource_id", resourceId)
                    .put("user", user)
                    .put("user_hash", usernameHasher.hash(user));

            final JSONObject dataEvent = new JSONObject()
                    .put("data_type", resourceType.name().toLowerCase())
                    .put("data_op", actionType.getShortname())
                    .put("data", payload);

            eventsProcessor.sendEvents(auditEventType,
                    Collections.singletonList(eventMetadata.addTo(dataEvent)));
        } catch (final Throwable e) {
            LOG.error("Error occurred when submitting audit event for publishing", e);
        }
    }

    private String serialize(final Object state) {
        try {
            return objectMapper.writeValueAsString(state);
        } catch (JsonProcessingException e) {
            LOG.error("failed to publish audit log", e);
            return null;
        }
    }

    public enum ResourceType {
        EVENT_TYPE,
        SUBSCRIPTION,
        TIMELINE,
        STORAGE,
        FEATURE,
        ADMINS,
        CURSORS,
        BLACKLIST_ENTRY
    }

    public enum ActionType {
        CREATED("C"), UPDATED("U"), DELETED("D");

        private final String shortname;

        ActionType(final String shortname) {
            this.shortname = shortname;
        }

        public String getShortname() {
            return shortname;
        }
    }
}
