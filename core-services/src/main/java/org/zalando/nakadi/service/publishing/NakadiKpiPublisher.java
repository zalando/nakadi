package org.zalando.nakadi.service.publishing;

import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.security.UsernameHasher;

import java.io.IOException;
import java.util.function.Supplier;

@Component
public class NakadiKpiPublisher {

    private static final Logger LOG = LoggerFactory.getLogger(NakadiKpiPublisher.class);

    private final EventsProcessor eventsProcessor;
    private final UsernameHasher usernameHasher;
    private final MetadataService metadataService;

    @Autowired
    protected NakadiKpiPublisher(
            final EventsProcessor eventsProcessor,
            final UsernameHasher usernameHasher,
            final MetadataService metadataService)
            throws IOException {
        this.eventsProcessor = eventsProcessor;
        this.usernameHasher = usernameHasher;
        this.metadataService = metadataService;
    }

    public void publish(final String etName, final Supplier<JSONObject> eventSupplier) {
        try {
            final JSONObject event = eventSupplier.get();
            metadataService.enrich(event);
            eventsProcessor.queueEvent(etName, event);
        } catch (final Exception e) {
            LOG.error("Error occurred when submitting KPI event for publishing", e);
        }
    }

    public String hash(final String value) {
        return usernameHasher.hash(value);
    }
}
