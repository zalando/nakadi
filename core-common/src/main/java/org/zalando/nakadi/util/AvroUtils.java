package org.zalando.nakadi.util;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

public class AvroUtils {

    public static Schema getParsedSchema(final String schema) throws AvroRuntimeException {
        return new Schema.Parser().parse(schema);
    }

    public static Schema getParsedSchema(final InputStream schema) throws AvroRuntimeException, IOException {
        return new Schema.Parser().parse(schema);
    }

    public static Schema getParsedSchema(
            final InputStream schema,
            final Map<String, Schema> types) throws AvroRuntimeException, IOException {
        return new Schema.Parser().addTypes(types).parse(schema);
    }

}
