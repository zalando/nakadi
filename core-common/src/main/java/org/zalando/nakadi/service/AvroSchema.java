package org.zalando.nakadi.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.avro.AvroMapper;
import org.apache.avro.Schema;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Service;
import org.zalando.nakadi.util.AvroUtils;

import java.io.IOException;

// temporarily storage for event type avro schemas untill schema repository supports them
@Service
public class AvroSchema {

    public static final byte METADATA_VERSION = 0;
    private final Schema metadataSchema;
    private final Schema nakadiAccessLogSchema;
    private final AvroMapper avroMapper;
    private final ObjectMapper objectMapper;

    @Autowired
    public AvroSchema(
            final AvroMapper avroMapper,
            final ObjectMapper objectMapper,
            @Value("${nakadi.avro.schema.metadata:classpath:metadata.avsc}")
            final Resource metadataRes,
            @Value("${nakadi.avro.schema.nakadi-access-log:classpath:nakadi.access.log.avsc}")
            final Resource nakadiAccessLogRes)
            throws IOException {
        this.avroMapper = avroMapper;
        this.objectMapper = objectMapper;
        this.metadataSchema = AvroUtils.getParsedSchema(metadataRes.getInputStream());
        this.nakadiAccessLogSchema = AvroUtils.getParsedSchema(nakadiAccessLogRes.getInputStream());
    }

    public Schema getNakadiAccessLogSchema() {
        return nakadiAccessLogSchema;
    }

    public Schema getMetadataSchema() {
        return metadataSchema;
    }

    public AvroMapper getAvroMapper() {
        return avroMapper;
    }

    public ObjectMapper getObjectMapper() {
        return objectMapper;
    }
}
