package org.zalando.nakadi.exceptions.runtime;

import org.apache.avro.AvroRuntimeException;

public class AvroDecodingException extends NakadiBaseException {
    public AvroDecodingException(
            final String message,
            final AvroRuntimeException exception) {
        super(message, exception);
    }
}
