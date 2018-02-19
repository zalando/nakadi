package org.zalando.nakadi.service;

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
import java.util.function.Supplier;

@Component
public class NakadiKpiPublisher {

    private static final Logger LOG = LoggerFactory.getLogger(NakadiKpiPublisher.class);

    private final ThreadLocal<MessageDigest> messageDigestThreadLocal;
    private final FeatureToggleService featureToggleService;
    private final EventsProcessor eventsProcessor;

    @Autowired
    protected NakadiKpiPublisher(final FeatureToggleService featureToggleService,
                                 final EventsProcessor eventsProcessor,
                                 @Value("${nakadi.hasher.salt}") final String salt) {
        this.featureToggleService = featureToggleService;
        this.eventsProcessor = eventsProcessor;
        messageDigestThreadLocal = ThreadLocal.withInitial(() -> {
            final MessageDigest sha256Digest = DigestUtils.getSha256Digest();
            sha256Digest.update(salt.getBytes());
            return sha256Digest;
        });
    }

    public void publish(final String etName, final Supplier<JSONObject> eventSupplier) {
        try {
            if (!featureToggleService.isFeatureEnabled(FeatureToggleService.Feature.KPI_COLLECTION)) {
                return;
            }

            final JSONObject event = eventSupplier.get();
            eventsProcessor.enrichAndSubmit(etName, event);
        } catch (final Exception e) {
            LOG.error("Error occurred when submitting KPI event for publishing", e);
        }
    }

    public String hash(final String value) {
        final MessageDigest messageDigest = messageDigestThreadLocal.get();
        return Hex.encodeHexString(messageDigest.digest(value.getBytes(Charsets.UTF_8)));
    }

}
