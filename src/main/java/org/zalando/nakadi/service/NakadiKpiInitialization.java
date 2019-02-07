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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
@ConfigurationProperties(prefix = "nakadi.kpi.event-types")
public class NakadiKpiInitialization {
    private static final Logger LOG = LoggerFactory.getLogger(NakadiKpiInitialization.class);

    private final ObjectMapper objectMapper;
    private final EventTypeService eventTypeService;
    private final FeatureToggleService featureToggleService;

    private String nakadiAccessLog;
    private String owningApplication;
    private String nakadiBatchPublished;
    private String nakadiDataStreamed;
    private String nakadiSubscriptionLog;
    private String nakadiEventTypeLog;

    @Autowired
    public NakadiKpiInitialization(final ObjectMapper objectMapper, final EventTypeService eventTypeService,
                                   final FeatureToggleService featureToggleService) {
        this.objectMapper = objectMapper;
        this.eventTypeService = eventTypeService;
        this.featureToggleService = featureToggleService;
    }

    @EventListener
    public void onApplicationEvent(final ContextRefreshedEvent event) throws IOException {
        if (!featureToggleService.isFeatureEnabled(FeatureToggleService.Feature.KPI_COLLECTION)) {
            LOG.debug("KPI collection is disabled, skip creation of kpi event types");
            return;
        }

        LOG.debug("Initializing KPI event types");

        String kpiEventTypesString = Resources
                .toString(Resources.getResource("kpi_event_types.json"), Charsets.UTF_8);

        final Map<String, String> replacements = new HashMap<>();
        replacements.put("nakadi.event.type.log", nakadiEventTypeLog);
        replacements.put("nakadi.subscription.log", nakadiSubscriptionLog);
        replacements.put("nakadi.data.streamed", nakadiDataStreamed);
        replacements.put("nakadi.batch.published", nakadiBatchPublished);
        replacements.put("nakadi.access.log", nakadiAccessLog);
        replacements.put("owning_application_placeholder", owningApplication);

        for (final Map.Entry<String, String> entry : replacements.entrySet()) {
            kpiEventTypesString = kpiEventTypesString.replaceAll(entry.getKey(), entry.getValue());
        }

        final TypeReference<List<EventTypeBase>> typeReference = new TypeReference<List<EventTypeBase>>() {
        };
        final List<EventTypeBase> eventTypes = objectMapper.readValue(kpiEventTypesString, typeReference);


        eventTypes.forEach(et -> {
            try {
                eventTypeService.create(et);
            } catch (final DuplicatedEventTypeNameException e) {
                LOG.debug("KPI event type already exists " + et.getName());
            } catch (final NakadiBaseException e) {
                LOG.debug("Problem creating KPI event type " + et.getName(), e);
            }
        });
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
