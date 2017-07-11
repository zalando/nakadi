package org.zalando.nakadi.service;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.domain.ConsumedEvent;
import org.zalando.nakadi.view.Cursor;
import org.zalando.nakadi.view.SubscriptionCursor;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Charsets.UTF_8;

@Component
@Qualifier("string")
public class EventStreamWriterString implements EventStreamWriter {

    @Override
    public int writeSubscriptionBatch(final OutputStream os, final SubscriptionCursor cursor,
                                      final List<ConsumedEvent> events,
                                      final Optional<String> metadata) throws IOException {
        final StringBuilder builder = new StringBuilder()
                .append("{\"cursor\":{\"partition\":\"").append(cursor.getPartition())
                .append("\",\"offset\":\"").append(cursor.getOffset())
                .append("\",\"event_type\":\"").append(cursor.getEventType())
                .append("\",\"cursor_token\":\"").append(cursor.getCursorToken())
                .append("\"}");
        if (!events.isEmpty()) {
            builder.append(",\"events\":[");
            events.forEach(event -> builder.append(new String(event.getEvent())).append(","));
            builder.deleteCharAt(builder.length() - 1).append("]");
        }
        metadata.ifPresent(s -> builder.append(",\"info\":{\"debug\":\"").append(s).append("\"}"));
        builder.append("}").append(BATCH_SEPARATOR);

        final String eventsString = builder.toString();

        final byte[] batchBytes = eventsString.getBytes(UTF_8);
        os.write(batchBytes);

        os.flush();

        return batchBytes.length;
    }

    @Override
    public int writeBatch(final OutputStream os, final Cursor cursor, final List<byte[]> events) throws IOException {
        final StringBuilder builder = new StringBuilder()
                .append("{\"cursor\":{\"partition\":\"").append(cursor.getPartition())
                .append("\",\"offset\":\"").append(cursor.getOffset()).append("\"}");
        if (!events.isEmpty()) {
            builder.append(",\"events\":[");
            events.forEach(event -> builder.append(new String(event)).append(","));
            builder.deleteCharAt(builder.length() - 1).append("]");
        }

        builder.append("}").append(BATCH_SEPARATOR);

        final String eventsString = builder.toString();

        final byte[] batchBytes = eventsString.getBytes(UTF_8);
        os.write(batchBytes);

        os.flush();

        return batchBytes.length;
    }
}
