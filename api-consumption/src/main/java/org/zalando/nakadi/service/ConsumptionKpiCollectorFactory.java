package org.zalando.nakadi.service;

import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.security.Client;
import org.zalando.nakadi.service.publishing.NakadiKpiPublisher;

@Component
public class ConsumptionKpiCollectorFactory {

    private final NakadiKpiPublisher kpiPublisher;
    private final String kpiDataStreamedEventType;
    private final long kpiCollectionIntervalMs;

    @Autowired
    public ConsumptionKpiCollectorFactory(
            final NakadiKpiPublisher kpiPublisher,
            @Value("${nakadi.kpi.event-types.nakadiDataStreamed}") final String kpiDataStreamedEventType,
            @Value("${nakadi.kpi.config.stream-data-collection-frequency-ms}") final long kpiCollectionIntervalMs) {
        this.kpiPublisher = kpiPublisher;
        this.kpiDataStreamedEventType = kpiDataStreamedEventType;
        this.kpiCollectionIntervalMs = kpiCollectionIntervalMs;
    }

    public ConsumptionKpiCollector createForLoLA(
            final Client client) {
        return new ConsumptionKpiCollector(client, kpiPublisher, kpiDataStreamedEventType, kpiCollectionIntervalMs) {
            @Override
            protected JSONObject enrich(final JSONObject o) {
                return o.put("api", "lola");
            }
        };
    }

    public ConsumptionKpiCollector createForHiLA(final String subscriptionId, final Client client) {

        return new ConsumptionKpiCollector(
                client,
                kpiPublisher,
                kpiDataStreamedEventType,
                kpiCollectionIntervalMs) {
            @Override
            protected JSONObject enrich(final JSONObject o) {
                return o.put("api", "hila")
                        .put("subscription", subscriptionId);
            }
        };
    }
}
