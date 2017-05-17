package org.zalando.nakadi.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.util.FeatureToggleService;

@Component
public class EventStreamWriterProvider {
    private final FeatureToggleService featureToggleService;
    private final EventStreamWriter binaryWriter;
    private final EventStreamWriter stringWriter;

    @Autowired
    public EventStreamWriterProvider(
            final FeatureToggleService featureToggleService,
            @Qualifier("binary") final EventStreamWriter binaryWriter,
            @Qualifier("string") final EventStreamWriter stringWriter) {
        this.featureToggleService = featureToggleService;
        this.binaryWriter = binaryWriter;
        this.stringWriter = stringWriter;
    }

    public EventStreamWriter getWriter() {
        if (featureToggleService.isFeatureEnabled(FeatureToggleService.Feature.SEND_BATCH_VIA_OUTPUT_STREAM)) {
            return binaryWriter;
        } else {
            return stringWriter;
        }
    }
}
