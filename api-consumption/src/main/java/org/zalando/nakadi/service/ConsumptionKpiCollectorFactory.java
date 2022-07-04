package org.zalando.nakadi.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.kpi.event.NakadiDataStreamed;
import org.zalando.nakadi.security.Client;
import org.zalando.nakadi.service.publishing.NakadiKpiPublisher;

@Component
public class ConsumptionKpiCollectorFactory {

    private final NakadiKpiPublisher kpiPublisher;
    private final long kpiCollectionIntervalMs;

    @Autowired
    public ConsumptionKpiCollectorFactory(
            final NakadiKpiPublisher kpiPublisher,
            @Value("${nakadi.kpi.config.stream-data-collection-frequency-ms}") final long kpiCollectionIntervalMs) {
        this.kpiPublisher = kpiPublisher;
        this.kpiCollectionIntervalMs = kpiCollectionIntervalMs;
    }

    public ConsumptionKpiCollector createForLoLA(final Client client) {
        return new ConsumptionKpiCollector(client, kpiPublisher, kpiCollectionIntervalMs) {
            @Override
            protected NakadiDataStreamed.Builder enrich(final NakadiDataStreamed.Builder builder) {
                builder.setApi("lola");
                return builder;
            }
        };
    }

    public ConsumptionKpiCollector createForHiLA(final String subscriptionId, final Client client) {

        return new ConsumptionKpiCollector(
                client,
                kpiPublisher,
                kpiCollectionIntervalMs) {
            @Override
            protected NakadiDataStreamed.Builder enrich(final NakadiDataStreamed.Builder builder) {
                builder.setApi("hila");
                builder.setSubscription(subscriptionId);
                return builder;
            }
        };
    }
}
