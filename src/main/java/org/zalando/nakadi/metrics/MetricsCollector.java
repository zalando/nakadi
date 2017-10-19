package org.zalando.nakadi.metrics;

import java.io.Closeable;

public interface MetricsCollector extends Closeable {
    interface Step extends Closeable {
        @Override
        void close();
    }

    Step start(String name, boolean closePrevious);


    @Override
    void close();
}
