package org.zalando.nakadi.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.domain.ConsumedEvent;
import org.zalando.nakadi.repository.kafka.KafkaRecordDeserializer;
import org.zalando.nakadi.view.Cursor;
import org.zalando.nakadi.view.SubscriptionCursor;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Optional;

import static java.nio.charset.StandardCharsets.UTF_8;

@Component
public class EventStreamJsonWriter implements EventStreamWriter {
    private static final byte[] B_BATCH_SEPARATOR = BATCH_SEPARATOR.getBytes(UTF_8);

    private static final byte[] B_CURSOR_PARTITION_BEGIN = "{\"cursor\":{\"partition\":\"".getBytes(UTF_8);
    private static final byte[] B_OFFSET_BEGIN = "\",\"offset\":\"".getBytes(UTF_8);
    private static final byte[] B_CURSOR_PARTITION_END = "\"}".getBytes(UTF_8);
    private static final byte[] B_EVENT_TYPE_BEGIN = "\",\"event_type\":\"".getBytes(UTF_8);
    private static final byte[] B_CURSOR_TOKEN_BEGIN = "\",\"cursor_token\":\"".getBytes(UTF_8);
    private static final byte[] B_CLOSE_CURLY_BRACKET = "}".getBytes(UTF_8);
    private static final byte[] B_EVENTS_ARRAY_BEGIN = ",\"events\":[".getBytes(UTF_8);
    private static final byte[] B_DEBUG_BEGIN = ",\"info\":{\"debug\":\"".getBytes(UTF_8);
    private static final byte[] B_DEBUG_END = "\"}".getBytes(UTF_8);

    private static final byte B_COMMA_DELIM = ',';
    private static final byte B_CLOSE_BRACKET = ']';
    private static final int B_FIXED_BYTE_COUNT = B_CURSOR_PARTITION_BEGIN.length
            + B_OFFSET_BEGIN.length
            + B_CURSOR_PARTITION_END.length
            + B_EVENTS_ARRAY_BEGIN.length
            + 1 // B_COMMA_DELIM
            + 1 // B_CLOSE_BRACKET
            + B_CLOSE_CURLY_BRACKET.length
            + 1; //B_BATCH_SEPARATOR

    private static final int B_FIXED_BYTE_COUNT_SUBSCRIPTION = B_CURSOR_PARTITION_BEGIN.length
            + B_OFFSET_BEGIN.length
            + B_EVENT_TYPE_BEGIN.length
            + B_CURSOR_TOKEN_BEGIN.length
            + B_CURSOR_PARTITION_END.length
            + B_EVENTS_ARRAY_BEGIN.length
            + 1 // B_COMMA_DELIM
            + 1 // B_CLOSE_BRACKET
            + B_CLOSE_CURLY_BRACKET.length
            + 1; //B_BATCH_SEPARATOR

    private final KafkaRecordDeserializer kafkaRecordDeserializer;

    @Autowired
    public EventStreamJsonWriter(final KafkaRecordDeserializer kafkaRecordDeserializer) {
        this.kafkaRecordDeserializer = kafkaRecordDeserializer;
    }

    @Override
    public long writeBatch(final OutputStream os, final Cursor cursor, final List<byte[]> events) throws IOException {
        int byteCount = B_FIXED_BYTE_COUNT;

        os.write(B_CURSOR_PARTITION_BEGIN);
        final byte[] partition = cursor.getPartition().getBytes(UTF_8);
        os.write(partition);
        byteCount += partition.length;
        os.write(B_OFFSET_BEGIN);
        final byte[] offset = cursor.getOffset().getBytes(UTF_8);
        os.write(offset);
        byteCount += offset.length;

        os.write(B_CURSOR_PARTITION_END);
        if (!events.isEmpty()) {
            os.write(B_EVENTS_ARRAY_BEGIN);
            for (int i = 0; i < events.size(); i++) {
                final byte[] event = events.get(i);
                os.write(kafkaRecordDeserializer.deserializeToJsonBytes(event));
                byteCount += event.length;
                if (i < (events.size() - 1)) {
                    os.write(B_COMMA_DELIM);
                } else {
                    os.write(B_CLOSE_BRACKET);
                }
            }
        }
        os.write(B_CLOSE_CURLY_BRACKET);
        os.write(B_BATCH_SEPARATOR);

        os.flush();

        return byteCount;
    }

    @Override
    public long writeSubscriptionBatch(final OutputStream os, final SubscriptionCursor cursor,
                                       final List<ConsumedEvent> events,
                                       final Optional<String> metadata) throws IOException {
        int byteCount = B_FIXED_BYTE_COUNT_SUBSCRIPTION;

        os.write(B_CURSOR_PARTITION_BEGIN);
        final byte[] partition = cursor.getPartition().getBytes(UTF_8);
        os.write(partition);
        byteCount += partition.length;
        os.write(B_OFFSET_BEGIN);
        final byte[] offset = cursor.getOffset().getBytes(UTF_8);
        os.write(offset);
        byteCount += offset.length;

        os.write(B_EVENT_TYPE_BEGIN);
        final byte[] eventType = cursor.getEventType().getBytes(UTF_8);
        os.write(eventType);
        byteCount += eventType.length;

        os.write(B_CURSOR_TOKEN_BEGIN);
        final byte[] cursorToken = cursor.getCursorToken().getBytes(UTF_8);
        os.write(cursorToken);
        byteCount += cursorToken.length;

        os.write(B_CURSOR_PARTITION_END);
        if (!events.isEmpty()) {
            os.write(B_EVENTS_ARRAY_BEGIN);
            for (int i = 0; i < events.size(); i++) {
                final byte[] event = events.get(i).getEvent();
                os.write(kafkaRecordDeserializer.deserializeToJsonBytes(event));
                byteCount += event.length;
                if (i < (events.size() - 1)) {
                    os.write(B_COMMA_DELIM);
                } else {
                    os.write(B_CLOSE_BRACKET);
                }
            }
        }
        if (metadata.isPresent()) {
            os.write(B_DEBUG_BEGIN);
            byteCount += B_DEBUG_BEGIN.length;

            final byte[] debug = metadata.get().getBytes(UTF_8);
            os.write(debug);
            byteCount += debug.length;

            os.write(B_DEBUG_END);
            byteCount += B_DEBUG_END.length;
        }
        os.write(B_CLOSE_CURLY_BRACKET);
        os.write(B_BATCH_SEPARATOR);

        os.flush();

        return byteCount;
    }
}
