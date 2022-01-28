package org.zalando.nakadi.service.publishing;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.avro.AvroMapper;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.Resource;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.service.AvroSchema;
import org.zalando.nakadi.util.UUIDGenerator;

import static org.zalando.nakadi.utils.TestUtils.buildDefaultEventType;

public class AvroEventPublisherTest extends EventPublisherTest {

    @Test
    public void testAvroEventWasSerialized() throws Exception {
        final Resource metadata = new DefaultResourceLoader().getResource("metadata.avsc");
        final Resource accessLog = new DefaultResourceLoader().getResource("nakadi.access.log.avsc");
        final AvroSchema avroSchema = new AvroSchema(new AvroMapper(), new ObjectMapper(), metadata, accessLog);
        final AvroEventPublisher eventPublisher = new AvroEventPublisher(timelineService,
                cache, timelineSync, nakadiSettings, new UUIDGenerator(), avroSchema);
        final EventType eventType = buildDefaultEventType();
        Mockito.when(cache.getEventType(eventType.getName())).thenReturn(eventType);

        final GenericRecord event = new GenericData.Record(avroSchema.getNakadiAccessLogSchema());
        event.put("method", "POST");
        event.put("path", "/event-types");
        event.put("query", "");
        event.put("app", "nakadi");
        event.put("app_hashed", "hashed-app");
        event.put("status_code", 201);
        event.put("response_time_ms", 10);

        eventPublisher.publishAvro(eventType.getName(), "nakadi", event);
        Mockito.verify(topicRepository).syncPostEvent(Mockito.any());
    }

}