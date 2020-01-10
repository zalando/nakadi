package org.zalando.nakadi.metrics;

public class StreamKpiData {
        private long bytesSent = 0;
        private long numberOfEventsSent = 0;

        public long getAndResetBytesSent() {
            final long tmp = bytesSent;
            bytesSent = 0;
            return tmp;
        }

        public long getAndResetNumberOfEventsSent() {
            final long tmp = numberOfEventsSent;
            numberOfEventsSent = 0;
            return tmp;
        }

        public void addBytesSent(final long bytes) {
            bytesSent = bytesSent + bytes;
        }

        public void addNumberOfEventsSent(final long count) {
            numberOfEventsSent = numberOfEventsSent + count;
        }
}
