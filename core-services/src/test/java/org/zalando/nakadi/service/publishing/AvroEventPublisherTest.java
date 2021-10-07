package org.zalando.nakadi.service.publishing;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.springframework.core.io.DefaultResourceLoader;
import org.zalando.nakadi.cache.EventTypeCache;
import org.zalando.nakadi.config.NakadiSettings;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.NakadiRecord;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.repository.kafka.KafkaTopicRepository;
import org.zalando.nakadi.service.timeline.TimelineService;
import org.zalando.nakadi.service.timeline.TimelineSync;
import org.zalando.nakadi.util.UUIDGenerator;

import java.io.IOException;
import java.util.UUID;

public class AvroEventPublisherTest {

    private final TimelineService timelineService = Mockito.mock(TimelineService.class);
    private final EventTypeCache eventTypeCache = Mockito.mock(EventTypeCache.class);
    private final TimelineSync timelineSync = Mockito.mock(TimelineSync.class);
    private final NakadiSettings nakadiSettings = Mockito.mock(NakadiSettings.class);
    private final UUIDGenerator uuidGenerator = Mockito.mock(UUIDGenerator.class);
    private final KafkaTopicRepository kafkaTopicRepository = Mockito.mock(KafkaTopicRepository.class);
    private final EventType eventType = Mockito.mock(EventType.class);
    private final Timeline timeline = Mockito.mock(Timeline.class);
    private final String etName = "nakadi.access.log";
    private final DefaultResourceLoader resourceLoader;

    private final AvroEventPublisher avroEventPublisher;

    public AvroEventPublisherTest() throws IOException {
        resourceLoader = new DefaultResourceLoader();
        avroEventPublisher = new AvroEventPublisher(
                timelineService,
                eventTypeCache,
                timelineSync,
                nakadiSettings,
                uuidGenerator,
                resourceLoader.getResource("classpath:nakadi.access.log.avsc"),
                resourceLoader.getResource("classpath:nakadi.metadata.avsc")
        );
    }

    @Before
    public void before() {
        Mockito.when(uuidGenerator.randomUUID())
                .thenReturn(UUID.fromString("0b3beccd-ce52-4f08-bff4-3a3945102d6b"));
        Mockito.when(eventTypeCache.getEventType(ArgumentMatchers.any()))
                .thenReturn(eventType);
        Mockito.when(timelineService.getActiveTimeline(eventType))
                .thenReturn(timeline);
        Mockito.when(timelineService.getTopicRepository(eventType))
                .thenReturn(kafkaTopicRepository);
    }

    @After
    public void after() {
        Mockito.reset(
                timelineService, eventTypeCache, timelineSync,
                nakadiSettings, uuidGenerator, kafkaTopicRepository
        );
    }

    @Test
    public void testEventAssembledWithMetadata() throws IOException {
        avroEventPublisher.publishAvro(
                etName,
                "POST",
                "/xxx/yyy",
                "?chotottam=xer",
                "adyachkov",
                "hashed_adyachkov",
                200,
                0L
        );

        final ArgumentCaptor<NakadiRecord> recordCaptor =
                ArgumentCaptor.forClass(NakadiRecord.class);
        Mockito.verify(kafkaTopicRepository).syncPostEvent(recordCaptor.capture());

        final GenericRecord record = deserializeGenericRecord(recordCaptor.getValue().getData());
        Assert.assertEquals("adyachkov", record.get("app").toString());
        Assert.assertEquals(
                "adyachkov",
                ((GenericRecord) record.get("metadata")).get("published_by").toString());
    }

    private GenericRecord deserializeGenericRecord(final byte[] data) throws IOException {
        final GenericDatumReader genericDatumReader = new GenericDatumReader(new Schema.Parser().parse(
                resourceLoader.getResource("classpath:nakadi.access.log.avsc").getInputStream()));
        return (GenericRecord) genericDatumReader.read(null,
                DecoderFactory.get().binaryDecoder(data, null));
    }
}