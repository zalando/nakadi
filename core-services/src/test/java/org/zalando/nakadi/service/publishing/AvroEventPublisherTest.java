package org.zalando.nakadi.service.publishing;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.avro.AvroMapper;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.Resource;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.NakadiRecord;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.service.AvroSchema;
import org.zalando.nakadi.util.FlowIdUtils;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.zalando.nakadi.utils.TestUtils.buildDefaultEventType;

public class AvroEventPublisherTest extends EventPublisherTest {

    @Test
    public void testAvroEventWasSerialized() throws Exception {
        final Resource metadataRes = new DefaultResourceLoader().getResource("metadata.avsc");
        final Resource accessLog = new DefaultResourceLoader().getResource("nakadi.access.log.avsc");
        final AvroSchema avroSchema = new AvroSchema(new AvroMapper(), new ObjectMapper(), metadataRes, accessLog);
        final AvroEventPublisher eventPublisher = new AvroEventPublisher(timelineService,
                cache, timelineSync, nakadiSettings);
        final EventType eventType = buildDefaultEventType();
        final String topic = UUID.randomUUID().toString();
        Mockito.when(cache.getEventType(eventType.getName())).thenReturn(eventType);
        Mockito.when(timelineService.getActiveTimeline(eventType))
                .thenReturn(new Timeline(eventType.getName(), 0, null, topic, null));

        final long now = System.currentTimeMillis();
        final GenericRecord metadata = new GenericRecordBuilder(
                avroSchema.getMetadataSchema())
                .set("occurred_at", now)
                .set("eid", "9702cf96-9bdb-48b7-9f4c-92643cb6d9fc")
                .set("flow_id", FlowIdUtils.peek())
                .set("event_type", eventType.getName())
                .set("partition", 0)
                .set("received_at", now)
                .set("schema_version", "0")
                .set("published_by", "adyachkov")
                .build();
        final GenericRecord event = new GenericRecordBuilder(
                avroSchema.getNakadiAccessLogSchema())
                .set("method", "POST")
                .set("path", "/event-types")
                .set("query", "")
                .set("app", "nakadi")
                .set("app_hashed", "hashed-app")
                .set("status_code", 201)
                .set("response_time_ms", 10)
                .build();

        final NakadiRecord nakadiRecord = NakadiRecord
                .fromAvro(eventType.getName(), metadata, event);
        final List<NakadiRecord> records = Collections.singletonList(nakadiRecord);
        eventPublisher.publishAvro(records);
        Mockito.verify(topicRepository).sendEvents(ArgumentMatchers.eq(topic), ArgumentMatchers.eq(records));
    }

}