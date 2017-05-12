package org.zalando.nakadi.service;

/*
 This is a full clone of EventStreamTest with SEND_BATCH_VIA_OUTPUT_STREAM enabled.

 todo: When the flag is lifted and the approach becomes the default, this can be removed.
  */
public class EventStreamTestSendBatchViaOutputStream extends EventStreamTest {
    @Override
    protected EventStreamWriter createWriter() {
        return new EventStreamWriterBinary();
    }
}
