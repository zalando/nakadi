package org.zalando.nakadi.metrics;

import java.util.concurrent.atomic.AtomicLong;

public class StreamKpiData {
        private AtomicLong bytesSent = new AtomicLong(0);
        private AtomicLong numberOfEventsSent = new AtomicLong(0);

        public long getAndResetBytesSent() {
            return bytesSent.getAndSet(0);
        }

        public long getAndResetNumberOfEventsSent() {
            return numberOfEventsSent.getAndSet(0);
        }

        public void addBytesSent(final long bytes) {
            bytesSent.addAndGet(bytes);
        }

        public void addNumberOfEventsSent(final long count) {
            numberOfEventsSent.addAndGet(count);
        }
}
