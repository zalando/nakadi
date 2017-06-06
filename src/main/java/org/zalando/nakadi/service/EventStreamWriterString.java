package org.zalando.nakadi.service;

import com.google.common.base.Function;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.domain.ConsumedEvent;
import org.zalando.nakadi.view.Cursor;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Charsets.UTF_8;

@Component
@Qualifier("string")
public class EventStreamWriterString implements EventStreamWriter {

    @Override
    public int writeSubscriptionBatch(final OutputStream os, final Cursor cursor,
                                      final List<ConsumedEvent> events,
                                      final Optional<String> metadata) throws IOException {
        return writeBatch(os, cursor, (List<Object>)(Object) events, metadata, (o) -> ((ConsumedEvent)o).getEvent());
    }

    @Override
    public int writeBatch(final OutputStream os, final Cursor cursor, final List<String> events) throws IOException {
        return writeBatch(os, cursor, (List<Object>)(Object)events, Optional.empty(), (o) -> (String)o);
    }

    private int writeBatch(final OutputStream os, final Cursor cursor, final List<Object> events,
                           final Optional<String> metadata,
                           final Function<Object, String> extrator) throws IOException {
        final StringBuilder builder = new StringBuilder()
                .append("{\"cursor\":{\"partition\":\"").append(cursor.getPartition())
                .append("\",\"offset\":\"").append(cursor.getOffset()).append("\"}");
        if (!events.isEmpty()) {
            builder.append(",\"events\":[");
            events.forEach(event -> builder.append(extrator.apply(event)).append(","));
            builder.deleteCharAt(builder.length() - 1).append("]");
        }
        metadata.ifPresent(s -> builder.append(",\"info\":{\"debug\":\"").append(s).append("\"}"));
        builder.append("}").append(BATCH_SEPARATOR);

        final String eventsString = builder.toString();

        final byte[] batchBytes = eventsString.getBytes(UTF_8);
        os.write(batchBytes);
        return batchBytes.length;
    }

}
