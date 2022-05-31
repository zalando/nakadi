package org.zalando.nakadi.service.publishing;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.avro.AvroMapper;
import org.json.JSONObject;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.Resource;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.Feature;
import org.zalando.nakadi.domain.NakadiRecord;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.domain.kpi.KPIEvent;
import org.zalando.nakadi.domain.kpi.SubscriptionLogEvent;
import org.zalando.nakadi.repository.TopicRepository;
import org.zalando.nakadi.repository.kafka.SequenceDecoder;
import org.zalando.nakadi.security.UsernameHasher;
import org.zalando.nakadi.service.LocalSchemaRegistry;
import org.zalando.nakadi.service.FeatureToggleService;
import org.zalando.nakadi.service.SchemaProviderService;
import org.zalando.nakadi.service.TestSchemaProviderService;
import org.zalando.nakadi.util.UUIDGenerator;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class NakadiKpiPublisherTest {

    private final FeatureToggleService featureToggleService = Mockito.mock(FeatureToggleService.class);
    private final JsonEventProcessor jsonProcessor = Mockito.mock(JsonEventProcessor.class);
    private final BinaryEventProcessor binaryProcessor = Mockito.mock(BinaryEventProcessor.class);
    private final LocalSchemaRegistry localRegistryMock = Mockito.mock(LocalSchemaRegistry.class);
    private final SchemaProviderService schemaProviderService = new TestSchemaProviderService(localRegistryMock);
    private final UUIDGenerator uuidGenerator = Mockito.mock(UUIDGenerator.class);
    private final UsernameHasher usernameHasher = new UsernameHasher("123");
    private final NakadiRecordMapper recordMapper = new NakadiRecordMapper(localRegistryMock);

    @Captor
    private ArgumentCaptor<String> eventTypeCaptor;
    @Captor
    private ArgumentCaptor<NakadiRecord> nakadiRecordCaptor;
    @Captor
    private ArgumentCaptor<JSONObject> jsonObjectCaptor;

    @Test
    public void testPublishJsonKPIEventWithFeatureToggleOn() {
        when(featureToggleService.isFeatureEnabled(Feature.KPI_COLLECTION)).thenReturn(true);
        when(featureToggleService.isFeatureEnabled(Feature.AVRO_FOR_KPI_EVENTS)).thenReturn(false);

        final var subscriptionLogEvent = new SubscriptionLogEvent()
                .setSubscriptionId("test-subscription-id")
                .setStatus("created");

        new NakadiKpiPublisher(featureToggleService, jsonProcessor, binaryProcessor, usernameHasher,
                new EventMetadataTestStub(), uuidGenerator, schemaProviderService, localRegistryMock, recordMapper)
                .publish(() -> subscriptionLogEvent);

        verify(jsonProcessor).queueEvent(eventTypeCaptor.capture(), jsonObjectCaptor.capture());

        assertEquals(subscriptionLogEvent.getName(), eventTypeCaptor.getValue());
        assertEquals("test-subscription-id", jsonObjectCaptor.getValue().get("subscription_id"));
        assertEquals("created", jsonObjectCaptor.getValue().get("status"));
        verifyNoInteractions(binaryProcessor, localRegistryMock);
    }

    @Test
    public void testPublishAvroKPIEventWithFeatureToggleOn() throws IOException {
        final var subscriptionLogEvent = new SubscriptionLogEvent()
                .setSubscriptionId("test-subscription-id")
                .setStatus("created");

        final var topicRepositoryMock = mock(TopicRepository.class);
        final var mockTimeline = mock(Timeline.class);
        final var eventType = new EventType();
        final var partitions = List.of("0", "1", "2");

        when(featureToggleService.isFeatureEnabled(Feature.KPI_COLLECTION)).thenReturn(true);
        when(featureToggleService.isFeatureEnabled(Feature.AVRO_FOR_KPI_EVENTS)).thenReturn(true);

        // Publish the above KPIEvent and capture it.
        final Resource eventTypeRes = new DefaultResourceLoader().getResource("event-type-schema/");
        final var localRegistry = new LocalSchemaRegistry(new AvroMapper(), new ObjectMapper(), eventTypeRes);
        new NakadiKpiPublisher(featureToggleService, jsonProcessor, binaryProcessor, usernameHasher,
                new EventMetadataTestStub(), new UUIDGenerator(),
                new TestSchemaProviderService(localRegistry), localRegistryMock, recordMapper)
                .publish(() -> subscriptionLogEvent);

        verifyNoInteractions(jsonProcessor);

        //Verify the event-type and NakadiRecord
        verify(binaryProcessor).queueEvent(eventTypeCaptor.capture(), nakadiRecordCaptor.capture());
        assertEquals(subscriptionLogEvent.getName(), eventTypeCaptor.getValue());
        final var nakadiRecord = nakadiRecordCaptor.getValue();
        assertEquals(subscriptionLogEvent.getName(), nakadiRecord.getMetadata().getEventType());

        // Build EnvelopHolder from the data in NakadiRecord and extract GenericRecord

        final var schemaEntry = localRegistry
                .getLatestEventTypeSchemaVersion(subscriptionLogEvent.getName());
        final var sequenceDecoder = new SequenceDecoder(schemaEntry.getSchema());
        final var record = sequenceDecoder.read(new ByteArrayInputStream(nakadiRecord.getPayload()));

        final var metadata = nakadiRecord.getMetadata();
        assertNotNull(metadata);

        // Verify values in GenericRecord
        assertEquals("test-subscription-id", record.get("subscription_id").toString());
        assertEquals("created", record.get("status").toString());
    }

    @Test
    public void testPublishKPIEventWithFeatureToggleOff() {
        when(featureToggleService.isFeatureEnabled(Feature.KPI_COLLECTION)).thenReturn(false);
        final Supplier<KPIEvent> mockEventSupplier = Mockito.mock(Supplier.class);
        new NakadiKpiPublisher(featureToggleService, jsonProcessor, binaryProcessor, usernameHasher,
                new EventMetadataTestStub(), uuidGenerator, schemaProviderService, localRegistryMock, recordMapper)
                .publish(mockEventSupplier);
        verifyNoInteractions(mockEventSupplier, jsonProcessor, binaryProcessor, uuidGenerator, localRegistryMock);
    }

    @Test
    public void testHash() throws Exception {
        final NakadiKpiPublisher publisher = new NakadiKpiPublisher(featureToggleService,
                jsonProcessor, binaryProcessor, usernameHasher,
                new EventMetadataTestStub(), uuidGenerator, schemaProviderService, localRegistryMock, recordMapper);

        assertThat(publisher.hash("application"),
                equalTo("befee725ab2ed3b17020112089a693ad8d8cfbf62b2442dcb5b89d66ce72391e"));
    }
}
