package org.zalando.nakadi.service.publishing;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class NamedThreadFactory implements ThreadFactory {
    private final String prefix;
    private final AtomicInteger counter = new AtomicInteger(0);

    public NamedThreadFactory(final String prefix) {
        this.prefix = prefix;
    }

    @Override
    public Thread newThread(final Runnable runnable) {
        final Thread t = new Thread(runnable);
        t.setName(this.prefix + counter.incrementAndGet());
        return t;
    }
}
