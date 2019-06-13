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
@ConfigurationProperties(prefix = "nakadi.kpi.event-types")
public class NakadiKpiInitialization {
    private static final Logger LOG = LoggerFactory.getLogger(NakadiKpiInitialization.class);

    private final SystemEventTypeInitializer systemEventTypeInitializer;
    private final FeatureToggleService featureToggleService;

    private String nakadiAccessLog;
    private String owningApplication;
    private String nakadiBatchPublished;
    private String nakadiDataStreamed;
    private String nakadiSubscriptionLog;
    private String nakadiEventTypeLog;

    @Autowired
    public NakadiKpiInitialization(
            final SystemEventTypeInitializer systemEventTypeInitializer,
            final FeatureToggleService featureToggleService) {
        this.systemEventTypeInitializer = systemEventTypeInitializer;
        this.featureToggleService = featureToggleService;
    }

    @EventListener
    public void onApplicationEvent(final ContextRefreshedEvent event) throws IOException {
        if (!featureToggleService.isFeatureEnabled(FeatureToggleService.Feature.KPI_COLLECTION)) {
            LOG.debug("KPI collection is disabled, skip creation of kpi event types");
            return;
        }
        final Map<String, String> replacements = new HashMap<>();
        replacements.put("nakadi.event.type.log", nakadiEventTypeLog);
        replacements.put("nakadi.subscription.log", nakadiSubscriptionLog);
        replacements.put("nakadi.data.streamed", nakadiDataStreamed);
        replacements.put("nakadi.batch.published", nakadiBatchPublished);
        replacements.put("nakadi.access.log", nakadiAccessLog);
        replacements.put("owning_application_placeholder", owningApplication);

        systemEventTypeInitializer.createEventTypesFromResource("kpi_event_types.json", replacements);
    }

    public String getNakadiAccessLog() {
        return nakadiAccessLog;
    }

    public void setNakadiAccessLog(final String nakadiAccessLog) {
        this.nakadiAccessLog = nakadiAccessLog;
    }

    public String getOwningApplication() {
        return owningApplication;
    }

    public void setOwningApplication(final String owningApplication) {
        this.owningApplication = owningApplication;
    }

    public String getNakadiBatchPublished() {
        return nakadiBatchPublished;
    }

    public void setNakadiBatchPublished(final String nakadiBatchPublished) {
        this.nakadiBatchPublished = nakadiBatchPublished;
    }

    public String getNakadiDataStreamed() {
        return nakadiDataStreamed;
    }

    public void setNakadiDataStreamed(final String nakadiDataStreamed) {
        this.nakadiDataStreamed = nakadiDataStreamed;
    }

    public String getNakadiSubscriptionLog() {
        return nakadiSubscriptionLog;
    }

    public void setNakadiSubscriptionLog(final String nakadiSubscriptionLog) {
        this.nakadiSubscriptionLog = nakadiSubscriptionLog;
    }

    public String getNakadiEventTypeLog() {
        return nakadiEventTypeLog;
    }

    public void setNakadiEventTypeLog(final String nakadiEventTypeLog) {
        this.nakadiEventTypeLog = nakadiEventTypeLog;
    }
}
