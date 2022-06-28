package org.zalando.nakadi.service;

import org.zalando.nakadi.domain.ConsumedEvent;
import org.zalando.nakadi.view.Cursor;
import org.zalando.nakadi.view.SubscriptionCursor;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Optional;

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
    long writeBatch(OutputStream os, Cursor cursor, List<byte[]> events) throws IOException;

    long writeSubscriptionBatch(OutputStream os, SubscriptionCursor cursor, List<ConsumedEvent> events,
                               Optional<String> metadata) throws IOException;
}
