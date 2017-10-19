package org.zalando.nakadi.metrics;

import java.io.Closeable;

public interface MetricsCollector extends Closeable {
    interface Step extends Closeable {
        @Override
        void close();
    }

    default Step closeStart(final String name) {
        return start(name, true);
    }

    default Step start(final String name) {
        return start(name, false);
    }

    Step start(String name, boolean closePrevious);

    void closeLast();

    @Override
    void close();
}
