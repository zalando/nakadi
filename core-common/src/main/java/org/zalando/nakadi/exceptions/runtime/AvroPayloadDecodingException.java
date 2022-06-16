package org.zalando.nakadi.exceptions.runtime;

import org.apache.avro.AvroRuntimeException;

public class AvroPayloadDecodingException extends NakadiBaseException {
    public AvroPayloadDecodingException(
            final String message,
            final AvroRuntimeException exception) {
        super(message, exception);
    }
}
