package org.zalando.nakadi.repository.kafka;


import com.google.common.base.Charsets;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;

public class EventAuthHeader extends RecordHeader {

    static final String HEADER_KEY = "X-Event-Auth";

    public EventAuthHeader(final String value) {
        super(HEADER_KEY, value.getBytes(Charsets.UTF_8));
    }

    public String getEventAuthValue() {
        return new String(value(), Charsets.UTF_8);
    }

    public static EventAuthHeader valueOf(final Header header) {
        if (HEADER_KEY.equals(header.key())) {
            return (EventAuthHeader) header;
        }

        throw new IllegalArgumentException(String.format("Can not parse header with key: %s", header.key()));
    }
}
