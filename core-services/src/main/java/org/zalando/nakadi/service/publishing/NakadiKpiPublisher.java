package org.zalando.nakadi.service.publishing;

import org.apache.avro.generic.GenericRecord;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.domain.Feature;
import org.zalando.nakadi.domain.NakadiAvroMetadata;
import org.zalando.nakadi.domain.NakadiRecord;
import org.zalando.nakadi.domain.kpi.AccessLogEvent;
import org.zalando.nakadi.domain.kpi.BatchPublishedEvent;
import org.zalando.nakadi.domain.kpi.DataStreamedEvent;
import org.zalando.nakadi.domain.kpi.EventTypeLogEvent;
import org.zalando.nakadi.domain.kpi.KPIEvent;
import org.zalando.nakadi.domain.kpi.SubscriptionLogEvent;
import org.zalando.nakadi.security.UsernameHasher;
import org.zalando.nakadi.service.FeatureToggleService;
import org.zalando.nakadi.service.KPIEventMapper;
import org.zalando.nakadi.service.LocalSchemaRegistry;
import org.zalando.nakadi.service.SchemaProviderService;
import org.zalando.nakadi.util.FlowIdUtils;
import org.zalando.nakadi.util.UUIDGenerator;

import java.util.Set;
import java.util.function.Supplier;

@Component
public class NakadiKpiPublisher {

    private static final Logger LOG = LoggerFactory.getLogger(NakadiKpiPublisher.class);

    private static final String VERSION_METADATA = "1";

    private final FeatureToggleService featureToggleService;
    private final JsonEventProcessor jsonEventsProcessor;
    private final BinaryEventProcessor binaryEventsProcessor;
    private final UsernameHasher usernameHasher;
    private final EventMetadata eventMetadata;
    private final UUIDGenerator uuidGenerator;
    private final SchemaProviderService schemaService;
    private final LocalSchemaRegistry localSchemaRegistry;
    private final KPIEventMapper kpiEventMapper;
    private final NakadiRecordMapper nakadiRecordMapper;

    @Autowired
    protected NakadiKpiPublisher(
            final FeatureToggleService featureToggleService,
            final JsonEventProcessor jsonEventsProcessor,
            final BinaryEventProcessor binaryEventsProcessor,
            final UsernameHasher usernameHasher,
            final EventMetadata eventMetadata,
            final UUIDGenerator uuidGenerator,
            final SchemaProviderService schemaService,
            final LocalSchemaRegistry localSchemaRegistry,
            final NakadiRecordMapper nakadiRecordMapper) {
        this.featureToggleService = featureToggleService;
        this.jsonEventsProcessor = jsonEventsProcessor;
        this.binaryEventsProcessor = binaryEventsProcessor;
        this.usernameHasher = usernameHasher;
        this.eventMetadata = eventMetadata;
        this.uuidGenerator = uuidGenerator;
        this.schemaService = schemaService;
        this.localSchemaRegistry = localSchemaRegistry;
        this.nakadiRecordMapper = nakadiRecordMapper;
        this.kpiEventMapper = new KPIEventMapper(Set.of(
                AccessLogEvent.class,
                SubscriptionLogEvent.class,
                EventTypeLogEvent.class,
                BatchPublishedEvent.class,
                DataStreamedEvent.class));
    }

    public void publish(final Supplier<KPIEvent> kpiEventSupplier) {
        try {
            if (!featureToggleService.isFeatureEnabled(Feature.KPI_COLLECTION)) {
                return;
            }
            final var kpiEvent = kpiEventSupplier.get();
            final var eventTypeName = kpiEvent.getName();

            if (featureToggleService.isFeatureEnabled(Feature.AVRO_FOR_KPI_EVENTS)) {
                final String eventVersion = schemaService.getAvroSchemaVersion(
                        eventTypeName, kpiEvent.getSchema());
                final NakadiAvroMetadata metadata = buildMetaData(
                        eventTypeName, VERSION_METADATA, eventVersion);
                final GenericRecord event = kpiEventMapper.mapToGenericRecord(kpiEvent);

                final NakadiRecord nakadiRecord =
                        nakadiRecordMapper.fromAvroGenericRecord(metadata, event);
                binaryEventsProcessor.queueEvent(eventTypeName, nakadiRecord);
            } else {
                final JSONObject eventObject = kpiEventMapper.mapToJsonObject(kpiEvent);
                jsonEventsProcessor.queueEvent(eventTypeName, eventMetadata.addTo(eventObject));
            }

        } catch (final Exception e) {
            LOG.error("Error occurred when submitting KPI event for publishing", e);
        }
    }

    private NakadiAvroMetadata buildMetaData(final String eventTypeName,
                                             final String metadataVersion,
                                             final String eventVersion) {
        final var metaSchemaEntry = localSchemaRegistry
                .getEventTypeSchema(LocalSchemaRegistry.METADATA_KEY, VERSION_METADATA);

        final var metadata = new NakadiAvroMetadata(
                Byte.parseByte(metadataVersion),
                metaSchemaEntry);
        metadata.setOccurredAt(System.currentTimeMillis());
        metadata.setEid(uuidGenerator.randomUUID().toString());
        metadata.setEventType(eventTypeName);
        metadata.setSchemaVersion(eventVersion);
        metadata.setFlowId(FlowIdUtils.peek());

        return metadata;
    }

    public String hash(final String value) {
        return usernameHasher.hash(value);
    }
}
