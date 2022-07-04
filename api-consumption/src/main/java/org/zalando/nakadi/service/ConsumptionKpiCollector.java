package org.zalando.nakadi.service;

import org.zalando.nakadi.kpi.event.NakadiDataStreamed;
import org.zalando.nakadi.security.Client;
import org.zalando.nakadi.service.publishing.NakadiKpiPublisher;

import java.util.HashMap;
import java.util.Map;

public abstract class ConsumptionKpiCollector {
    private final String clientId;
    private final String clientRealm;
    private final String appNameHashed;
    private final NakadiKpiPublisher kpiPublisher;
    private final long kpiFlushIntervalMs;

    private final Map<String, StreamKpiData> kpiDataPerEventType = new HashMap<>();
    private long lastKpiEventSent = System.currentTimeMillis();

    public ConsumptionKpiCollector(
            final Client client,
            final NakadiKpiPublisher kpiPublisher,
            final long kpiFlushIntervalMs) {
        this.clientId = client.getClientId();
        this.clientRealm = client.getRealm();
        this.appNameHashed = kpiPublisher.hash(clientId);
        this.kpiPublisher = kpiPublisher;
        this.kpiFlushIntervalMs = kpiFlushIntervalMs;
    }

    public void sendKpi() {
        kpiDataPerEventType.forEach(this::publishKpi);
        kpiDataPerEventType.clear();
    }

    public void checkAndSendKpi() {
        if ((System.currentTimeMillis() - lastKpiEventSent) > kpiFlushIntervalMs) {
            sendKpi();
            lastKpiEventSent = System.currentTimeMillis();
        }
    }

    public void recordBatchSent(final String eventType, final long bytesCount, final int eventsCount) {
        final StreamKpiData kpiData = kpiDataPerEventType.computeIfAbsent(eventType, (x) -> new StreamKpiData());
        kpiData.bytesSent += bytesCount;
        kpiData.numberOfEventsSent += eventsCount;
        kpiData.batchesCount += 1;
    }

    protected abstract NakadiDataStreamed.Builder enrich(NakadiDataStreamed.Builder dataStreamedEvent);

    private void publishKpi(final String eventType, final StreamKpiData data) {
        kpiPublisher.publish(() -> {
            final NakadiDataStreamed.Builder builder = NakadiDataStreamed.newBuilder()
                    .setEventType(eventType)
                    .setApp(clientId)
                    .setAppHashed(appNameHashed)
                    .setTokenRealm(clientRealm)
                    .setNumberOfEvents(data.numberOfEventsSent)
                    .setBytesStreamed(data.bytesSent)
                    .setBatchesStreamed(data.batchesCount);
            return enrich(builder).build();
        });
    }

    private static class StreamKpiData {
        private long bytesSent = 0;
        private long numberOfEventsSent = 0;
        private int batchesCount = 0;
    }
}
