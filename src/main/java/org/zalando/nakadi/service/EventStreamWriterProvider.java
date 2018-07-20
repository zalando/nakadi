package org.zalando.nakadi.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Component
public class EventStreamWriterProvider {
    private final FeatureToggleService featureToggleService;
    private final EventStreamWriter binaryWriter;

    @Autowired
    public EventStreamWriterProvider(
            final FeatureToggleService featureToggleService,
            @Qualifier("binary") final EventStreamWriter binaryWriter) {
        this.featureToggleService = featureToggleService;
        this.binaryWriter = binaryWriter;
    }

    public EventStreamWriter getWriter() {
        return binaryWriter;
    }
}
