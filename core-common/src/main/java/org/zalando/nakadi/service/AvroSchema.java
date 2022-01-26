package org.zalando.nakadi.service;

import org.apache.avro.Schema;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Service;

import java.io.IOException;

// temporarily storage for event type avro schemas untill schema repository supports them
@Service
public class AvroSchema {

    public static final byte METADATA_VERSION = 0;
    private final Schema metadataSchema;
    private final Schema nakadiAccessLogSchema;

    @Autowired
    public AvroSchema(
            @Value("${nakadi.schema.metadata:classpath:metadata.avsc}")
            final Resource metadataRes,
            @Value("${nakadi.schema.nakadi-access-log:classpath:nakadi.access.log.avsc}")
            final Resource nakadiAccessLogRes)
            throws IOException {
        this.metadataSchema = new Schema.Parser().parse(metadataRes.getInputStream());
        this.nakadiAccessLogSchema = new Schema.Parser().parse(nakadiAccessLogRes.getInputStream());
    }

    public Schema getNakadiAccessLogSchema() {
        return nakadiAccessLogSchema;
    }

    public Schema getMetadataSchema() {
        return metadataSchema;
    }
}
