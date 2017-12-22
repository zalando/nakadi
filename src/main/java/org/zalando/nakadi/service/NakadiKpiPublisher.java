package org.zalando.nakadi.service;

import com.google.common.base.Charsets;
import org.apache.commons.codec.binary.Hex;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.util.FeatureToggleService;

import java.security.MessageDigest;
import java.util.function.Supplier;

@Component
public class NakadiKpiPublisher {

    private static final Logger LOG = LoggerFactory.getLogger(NakadiKpiPublisher.class);

    private final FeatureToggleService featureToggleService;
    private final EventsProcessor eventsProcessor;
    private final MessageDigest sha256MessageDigest;

    @Autowired
    protected NakadiKpiPublisher(final FeatureToggleService featureToggleService,
                                 final EventsProcessor eventsProcessor,
                                 final MessageDigest sha256MessageDigest) {
        this.featureToggleService = featureToggleService;
        this.eventsProcessor = eventsProcessor;
        this.sha256MessageDigest = sha256MessageDigest;
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
        return Hex.encodeHexString(sha256MessageDigest.digest(value.getBytes(Charsets.UTF_8)));
    }

}
