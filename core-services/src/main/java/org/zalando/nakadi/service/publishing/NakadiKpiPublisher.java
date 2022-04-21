package org.zalando.nakadi.service.publishing;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.cache.EventTypeCache;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.Feature;
import org.zalando.nakadi.domain.NakadiRecord;
import org.zalando.nakadi.domain.kpi.AccessLogEvent;
import org.zalando.nakadi.domain.kpi.BatchPublishedEvent;
import org.zalando.nakadi.domain.kpi.DataStreamedEvent;
import org.zalando.nakadi.domain.kpi.EventTypeLogEvent;
import org.zalando.nakadi.domain.kpi.KPIEvent;
import org.zalando.nakadi.domain.kpi.SubscriptionLogEvent;
import org.zalando.nakadi.security.UsernameHasher;
import org.zalando.nakadi.service.AvroSchema;
import org.zalando.nakadi.service.FeatureToggleService;
import org.zalando.nakadi.service.KPIEventMapper;
import org.zalando.nakadi.service.timeline.TimelineService;
import org.zalando.nakadi.util.FlowIdUtils;
import org.zalando.nakadi.util.UUIDGenerator;

import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.function.Supplier;

@Component
public class NakadiKpiPublisher {

    private static final Logger LOG = LoggerFactory.getLogger(NakadiKpiPublisher.class);

    private final FeatureToggleService featureToggleService;
    private final JsonEventProcessor jsonEventsProcessor;
    private final BinaryEventProcessor binaryEventsProcessor;
    private final UsernameHasher usernameHasher;
    private final EventMetadata eventMetadata;
    private final UUIDGenerator uuidGenerator;
    private final AvroSchema avroSchema;
    private final KPIEventMapper kpiEventMapper;
    private final NakadiRecordMapper nakadiRecordMapper;
    private final EventTypeCache eventTypeCache;
    private final TimelineService timelineService;

    @Autowired
    protected NakadiKpiPublisher(
            final FeatureToggleService featureToggleService,
            final JsonEventProcessor jsonEventsProcessor,
            final BinaryEventProcessor binaryEventsProcessor,
            final UsernameHasher usernameHasher,
            final EventMetadata eventMetadata,
            final UUIDGenerator uuidGenerator,
            final AvroSchema avroSchema,
            final NakadiRecordMapper nakadiRecordMapper,
            final EventTypeCache eventTypeCache,
            final TimelineService timelineService) {
        this.featureToggleService = featureToggleService;
        this.jsonEventsProcessor = jsonEventsProcessor;
        this.binaryEventsProcessor = binaryEventsProcessor;
        this.usernameHasher = usernameHasher;
        this.eventMetadata = eventMetadata;
        this.uuidGenerator = uuidGenerator;
        this.avroSchema = avroSchema;
        this.eventTypeCache = eventTypeCache;
        this.timelineService = timelineService;
        this.kpiEventMapper = new KPIEventMapper(Set.of(
                AccessLogEvent.class,
                SubscriptionLogEvent.class,
                EventTypeLogEvent.class,
                BatchPublishedEvent.class,
                DataStreamedEvent.class));
        this.nakadiRecordMapper = nakadiRecordMapper;
    }

    public void publish(final Supplier<KPIEvent> kpiEventSupplier) {
        try {
            if (!featureToggleService.isFeatureEnabled(Feature.KPI_COLLECTION)) {
                return;
            }
            final var kpiEvent = kpiEventSupplier.get();
            final var eventTypeName = kpiEvent.getName();
            final var eventType = eventTypeCache.getEventType(eventTypeName);
            final var partition = calculateRandomPartition(eventType);

            if (featureToggleService.isFeatureEnabled(Feature.AVRO_FOR_KPI_EVENTS)) {

                final var metaSchemaEntry = avroSchema
                        .getLatestEventTypeSchemaVersion(AvroSchema.METADATA_KEY);
                final var metadataVersion = Byte.parseByte(metaSchemaEntry.getVersion());

                final var eventSchemaEntry = avroSchema
                        .getLatestEventTypeSchemaVersion(eventTypeName);

                final GenericRecord metadata = buildMetaDataGenericRecord(
                        eventTypeName, partition, metaSchemaEntry.getSchema(), eventSchemaEntry.getVersion());

                final GenericRecord event = kpiEventMapper.mapToGenericRecord(kpiEvent, eventSchemaEntry.getSchema());

                final NakadiRecord nakadiRecord = nakadiRecordMapper.fromAvroGenericRecord(
                        metadataVersion, metadata, event);
                binaryEventsProcessor.queueEvent(eventTypeName, nakadiRecord);
            } else {
                final JSONObject eventObject = kpiEventMapper.mapToJsonObject(kpiEvent);
                jsonEventsProcessor.queueEvent(eventTypeName, eventMetadata.addTo(eventObject));
            }

        } catch (final Exception e) {
            LOG.error("Error occurred when submitting KPI event for publishing", e);
        }
    }

    private GenericRecord buildMetaDataGenericRecord(
            final String eventType, final int partition, final Schema schema, final String version) {
        return buildMetaDataGenericRecord(eventType, partition, schema, version, "unknown");
    }

    private GenericRecord buildMetaDataGenericRecord(
            final String eventType, final int partition, final Schema schema, final String version, final String user) {
        final long now = System.currentTimeMillis();
        return new GenericRecordBuilder(schema)
                .set("occurred_at", now)
                .set("eid", uuidGenerator.randomUUID().toString())
                .set("flow_id", FlowIdUtils.peek())
                .set("event_type", eventType)
                .set("partition", String.valueOf(partition)) // fixme avro
                .set("received_at", now)
                .set("schema_version", version)
                .set("published_by", user)
                .build();
    }

    public String hash(final String value) {
        return usernameHasher.hash(value);
    }

    private int calculateRandomPartition(final EventType eventType) {
        final var random = new Random();
        final List<String> partitions = timelineService.getTopicRepository(eventType)
                .listPartitionNames(timelineService.getActiveTimeline(eventType).getTopic());
        if (partitions.size() == 1) {
            return Integer.parseInt(partitions.get(0));
        } else {
            final int partitionIndex = random.nextInt(partitions.size());
            return Integer.parseInt(partitions.get(partitionIndex));
        }
    }
}
