package org.zalando.nakadi.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.security.MessageDigest;
import java.util.Optional;

@Component
public class NakadiAuditLogPublisher {

    private static final Logger LOG = LoggerFactory.getLogger(NakadiAuditLogPublisher.class);

    private final ThreadLocal<MessageDigest> messageDigestThreadLocal;
    private final FeatureToggleService featureToggleService;
    private final EventsProcessor eventsProcessor;
    private final byte[] salt;
    private final String auditEventType;
    private final ObjectMapper objectMapper;

    @Autowired
    protected NakadiAuditLogPublisher(final FeatureToggleService featureToggleService,
                                      final EventsProcessor eventsProcessor,
                                      final ObjectMapper objectMapper,
                                      @Value("${nakadi.hasher.salt}") final String salt,
                                      @Value("${nakadi.audit.eventType}") final String auditEventType) {
        this.featureToggleService = featureToggleService;
        this.eventsProcessor = eventsProcessor;
        this.salt = salt.getBytes(Charsets.UTF_8);
        this.messageDigestThreadLocal = ThreadLocal.withInitial(DigestUtils::getSha256Digest);
        this.objectMapper = objectMapper;
        this.auditEventType = auditEventType;
    }

    public void publish(final Optional<Object> previousState, final Optional<Object> newState,
                        final ResourceType resourceType, final ActionType actionType, final String resourceId,
                        final Optional<String> user) {
        try {
            if (!featureToggleService.isFeatureEnabled(FeatureToggleService.Feature.AUDIT_LOG_COLLECTION)) {
                return;
            }

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
                    .put("user", user.orElse(null))
                    .put("user_hash", user.map(this::hash).orElse(null));

            final JSONObject dataEvent = new JSONObject()
                    .put("data_type", resourceType.name().toLowerCase())
                    .put("data_op", actionType.getShortname())
                    .put("data", payload);


            eventsProcessor.enrichAndSubmit(auditEventType, dataEvent);
        } catch (final Throwable e) {
            LOG.error("Error occurred when submitting audit event for publishing", e);
        }
    }

    public String hash(final String value) {
        final MessageDigest messageDigest = messageDigestThreadLocal.get();
        messageDigest.reset();
        messageDigest.update(salt);
        messageDigest.update(value.getBytes(Charsets.UTF_8));
        return Hex.encodeHexString(messageDigest.digest());
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
        ADMINS
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
