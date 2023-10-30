package org.zalando.nakadi.service.publishing;

import org.apache.avro.message.RawMessageDecoder;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecord;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.zalando.nakadi.domain.Feature;
import org.zalando.nakadi.domain.NakadiRecord;
import org.zalando.nakadi.kpi.event.NakadiSubscriptionLog;
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
    private final UUIDGenerator uuidGenerator = new UUIDGenerator();
    private final UsernameHasher usernameHasher = new UsernameHasher("123");
    private final NakadiRecordMapper recordMapper;

    @Captor
    private ArgumentCaptor<String> eventTypeCaptor;
    @Captor
    private ArgumentCaptor<NakadiRecord> nakadiRecordCaptor;

    public NakadiKpiPublisherTest() throws IOException {
        this.recordMapper = TestUtils.getNakadiRecordMapper();
    }

    @Test
    public void testPublishKPIEventWithFeatureToggleOn() throws IOException {
        when(featureToggleService.isFeatureEnabled(Feature.KPI_COLLECTION)).thenReturn(true);

        final NakadiSubscriptionLog subscriptionLogEvent = NakadiSubscriptionLog.newBuilder()
                .setSubscriptionId("test-subscription-id")
                .setStatus("created")
                .build();

        new NakadiKpiPublisher(featureToggleService, binaryProcessor, usernameHasher,
                uuidGenerator, schemaProviderService, recordMapper)
                .publish(() -> subscriptionLogEvent);

        verify(binaryProcessor).queueEvent(eventTypeCaptor.capture(), nakadiRecordCaptor.capture());

        final NakadiRecord nakadiRecord = nakadiRecordCaptor.getValue();

        final NakadiSubscriptionLog result = new RawMessageDecoder<NakadiSubscriptionLog>(
                SpecificData.get(), NakadiSubscriptionLog.getClassSchema())
                .decode(nakadiRecord.getPayload());

        Assert.assertEquals("nakadi.subscription.log", eventTypeCaptor.getValue());
        Assert.assertEquals("nakadi.subscription.log", nakadiRecord.getMetadata().getEventType());
        Assert.assertEquals("test-subscription-id", result.getSubscriptionId());
        Assert.assertEquals("created", result.getStatus());
    }

    @Test
    public void testPublishKPIEventWithFeatureToggleOff() {
        when(featureToggleService.isFeatureEnabled(Feature.KPI_COLLECTION)).thenReturn(false);
        final Supplier<SpecificRecord> mockEventSupplier = Mockito.mock(Supplier.class);
        new NakadiKpiPublisher(featureToggleService, binaryProcessor, usernameHasher,
                uuidGenerator, schemaProviderService, recordMapper)
                .publish(mockEventSupplier);
        verifyNoInteractions(mockEventSupplier, jsonProcessor, binaryProcessor, localRegistryMock);
    }

    @Test
    public void testHash() {
        final NakadiKpiPublisher publisher = new NakadiKpiPublisher(featureToggleService,
                binaryProcessor, usernameHasher, uuidGenerator, schemaProviderService, recordMapper);

        assertThat(publisher.hash("application"),
                equalTo("befee725ab2ed3b17020112089a693ad8d8cfbf62b2442dcb5b89d66ce72391e"));
    }
}
