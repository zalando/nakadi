package org.zalando.nakadi.service.publishing;

import org.apache.avro.specific.SpecificRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.domain.Feature;
import org.zalando.nakadi.domain.NakadiMetadata;
import org.zalando.nakadi.domain.NakadiRecord;
import org.zalando.nakadi.kpi.event.NakadiAccessLog;
import org.zalando.nakadi.kpi.event.NakadiBatchPublished;
import org.zalando.nakadi.kpi.event.NakadiDataStreamed;
import org.zalando.nakadi.kpi.event.NakadiEventTypeLog;
import org.zalando.nakadi.kpi.event.NakadiSubscriptionLog;
import org.zalando.nakadi.mapper.NakadiRecordMapper;
import org.zalando.nakadi.security.UsernameHasher;
import org.zalando.nakadi.service.FeatureToggleService;
import org.zalando.nakadi.service.SchemaProviderService;
import org.zalando.nakadi.util.MDCUtils;
import org.zalando.nakadi.util.UUIDGenerator;

import java.time.Instant;
import java.util.Map;
import java.util.function.Supplier;

@Component
public class NakadiKpiPublisher {

    private static final Logger LOG = LoggerFactory.getLogger(NakadiKpiPublisher.class);

    private final Map<Class, String> classToEventTypeName;

    private final FeatureToggleService featureToggleService;
    private final BinaryEventProcessor binaryEventsProcessor;
    private final UsernameHasher usernameHasher;
    private final UUIDGenerator uuidGenerator;
    private final SchemaProviderService schemaService;
    private final NakadiRecordMapper nakadiRecordMapper;

    @Autowired
    protected NakadiKpiPublisher(
            final FeatureToggleService featureToggleService,
            final BinaryEventProcessor binaryEventsProcessor,
            final UsernameHasher usernameHasher,
            final UUIDGenerator uuidGenerator,
            final SchemaProviderService schemaService,
            final NakadiRecordMapper nakadiRecordMapper) {
        this.featureToggleService = featureToggleService;
        this.binaryEventsProcessor = binaryEventsProcessor;
        this.usernameHasher = usernameHasher;
        this.uuidGenerator = uuidGenerator;
        this.schemaService = schemaService;
        this.nakadiRecordMapper = nakadiRecordMapper;
        this.classToEventTypeName = Map.of(
                NakadiAccessLog.class, "nakadi.access.log",
                NakadiBatchPublished.class, "nakadi.batch.published",
                NakadiDataStreamed.class, "nakadi.data.streamed",
                NakadiEventTypeLog.class, "nakadi.event.type.log",
                NakadiSubscriptionLog.class, "nakadi.subscription.log"
        );
    }

    public void publish(final Supplier<SpecificRecord> kpiEventSupplier) {
        try {
            if (!featureToggleService.isFeatureEnabled(Feature.KPI_COLLECTION)) {
                return;
            }
            final var kpiEvent = kpiEventSupplier.get();
            final var eventTypeName = classToEventTypeName.get(kpiEvent.getClass());
            // fixme the NPE happens if new event type added, but name mapping forgotten 'classToEventTypeName'

            final String eventVersion = schemaService.getAvroSchemaVersion(
                    eventTypeName, kpiEvent.getSchema());
            final NakadiMetadata metadata = buildMetadata(eventTypeName, eventVersion);

            final NakadiRecord nakadiRecord =
                    nakadiRecordMapper.fromAvroRecord(metadata, kpiEvent);
            binaryEventsProcessor.queueEvent(eventTypeName, nakadiRecord);
        } catch (final Exception e) {
            LOG.error("Error occurred when submitting KPI event for publishing", e);
        }
    }

    private NakadiMetadata buildMetadata(final String eventTypeName,
                                         final String eventVersion) {
        final NakadiMetadata metadata = new NakadiMetadata();
        metadata.setOccurredAt(Instant.now());
        metadata.setEid(uuidGenerator.randomUUID().toString());
        metadata.setEventType(eventTypeName);
        metadata.setSchemaVersion(eventVersion);
        metadata.setFlowId(MDCUtils.getFlowId());

        return metadata;
    }

    public String hash(final String value) {
        return usernameHasher.hash(value);
    }
}
