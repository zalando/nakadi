package org.zalando.nakadi.service;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.view.Cursor;
import static com.google.common.base.Charsets.UTF_8;

@Component
@Qualifier("string")
public class EventStreamWriterString implements EventStreamWriter {

    public int writeBatch(final OutputStream os, final Cursor cursor, final List<String> events) throws IOException {
        final StringBuilder builder = new StringBuilder()
                .append("{\"cursor\":{\"partition\":\"").append(cursor.getPartition())
                .append("\",\"offset\":\"").append(cursor.getOffset()).append("\"}");
        if (!events.isEmpty()) {
            builder.append(",\"events\":[");
            events.forEach(event -> builder.append(event).append(","));
            builder.deleteCharAt(builder.length() - 1).append("]");
        }

        builder.append("}").append(BATCH_SEPARATOR);

        final String eventsString = builder.toString();

        final byte[] batchBytes = eventsString.getBytes(UTF_8);
        os.write(batchBytes);
        return batchBytes.length;
    }

}
