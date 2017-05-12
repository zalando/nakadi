package org.zalando.nakadi.service;

public class EventStreamTestSendBatchViaString extends EventStreamTest {
    @Override
    protected EventStreamWriter createWriter() {
        return new EventStreamWriterString();
    }
}
