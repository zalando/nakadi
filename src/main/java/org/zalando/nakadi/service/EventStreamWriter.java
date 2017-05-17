package org.zalando.nakadi.service;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import org.zalando.nakadi.view.Cursor;

public interface EventStreamWriter {

    String BATCH_SEPARATOR = "\n";

    /**
     * Writes batch to stream
     *
     * @param os     Stream to write to
     * @param cursor Cursor associated with this branch
     * @param events Events in batch
     * @return count of bytes written
     */
    int writeBatch(OutputStream os, Cursor cursor, List<String> events) throws IOException;

}
