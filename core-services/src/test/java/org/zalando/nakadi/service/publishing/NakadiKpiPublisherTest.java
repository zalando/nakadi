package org.zalando.nakadi.service.publishing;

import org.json.JSONObject;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.zalando.nakadi.domain.Feature;
import org.zalando.nakadi.domain.NakadiRecord;
import org.zalando.nakadi.domain.kpi.KPIEvent;
import org.zalando.nakadi.domain.kpi.SubscriptionLogEvent;
import org.zalando.nakadi.mapper.NakadiRecordMapper;
import org.zalando.nakadi.security.UsernameHasher;
import org.zalando.nakadi.service.FeatureToggleService;
import org.zalando.nakadi.service.LocalSchemaRegistry;
import org.zalando.nakadi.service.SchemaProviderService;
import org.zalando.nakadi.service.TestSchemaProviderService;
import org.zalando.nakadi.util.UUIDGenerator;
import org.zalando.nakadi.utils.TestUtils;

import java.io.IOException;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
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
    private final NakadiRecordMapper recordMapper;

    @Captor
    private ArgumentCaptor<String> eventTypeCaptor;
    @Captor
    private ArgumentCaptor<NakadiRecord> nakadiRecordCaptor;
    @Captor
    private ArgumentCaptor<JSONObject> jsonObjectCaptor;

    public NakadiKpiPublisherTest() throws IOException {
        this.recordMapper = TestUtils.getNakadiRecordMapper();
    }

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
