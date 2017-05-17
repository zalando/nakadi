package org.zalando.nakadi.service;

import static com.google.common.base.Charsets.UTF_8;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.view.Cursor;

@Component
@Qualifier("binary")
class EventStreamWriterBinary implements EventStreamWriter {
    private static final byte[] B_BATCH_SEPARATOR = BATCH_SEPARATOR.getBytes(UTF_8);

    private static final byte[] B_CURSOR_PARTITION_BEGIN = "{\"cursor\":{\"partition\":\"".getBytes(UTF_8);
    private static final byte[] B_OFFSET_BEGIN = "\",\"offset\":\"".getBytes(UTF_8);
    private static final byte[] B_CURSOR_PARTITION_END = "\"}".getBytes(UTF_8);
    private static final byte[] B_CLOSE_CURLY_BRACKET = "}".getBytes(UTF_8);
    private static final byte[] B_EVENTS_ARRAY_BEGIN = ",\"events\":[".getBytes(UTF_8);

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

    @Override
    public int writeBatch(final OutputStream os, final Cursor cursor, final List<String> events) throws IOException {

        int byteCount = B_FIXED_BYTE_COUNT;

        os.write(B_CURSOR_PARTITION_BEGIN);
        final byte[] partition = cursor.getPartition().getBytes(StandardCharsets.UTF_8);
        os.write(partition);
        byteCount += partition.length;
        os.write(B_OFFSET_BEGIN);
        final byte[] offset = cursor.getOffset().getBytes(StandardCharsets.UTF_8);
        os.write(offset);
        byteCount += offset.length;
        os.write(B_CURSOR_PARTITION_END);
        if (!events.isEmpty()) {
            os.write(B_EVENTS_ARRAY_BEGIN);
            for (int i = 0; i < events.size(); i++) {
                final byte[] event = events.get(i).getBytes(StandardCharsets.UTF_8);
                os.write(event);
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
        return byteCount;
    }
}
