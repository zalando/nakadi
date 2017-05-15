package org.zalando.nakadi.service;

public class EventStreamSendBatchViaStringTest extends EventStreamTest {
    @Override
    protected EventStreamWriter createWriter() {
        return new EventStreamWriterString();
    }
}
