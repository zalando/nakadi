package org.zalando.nakadi.metrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

@Service
public class NakadiKPIMetrics {
    public static final int EVENT_PACK_SIZE = 1000;
    private static final int QUEUE_SIZE = 100 * EVENT_PACK_SIZE;
    private static final Logger LOG = LoggerFactory.getLogger(NakadiKPIMetrics.class);

    private final ThreadLocal<List<MetricsCollectorImpl>> threadValuesHolder = new ThreadLocal<>();
    private final BlockingQueue<MetricsCollectorImpl> metrics = new LinkedBlockingQueue<>(QUEUE_SIZE);


    public MetricsCollector startCollection(final String et, final Map<String, String> additional) {
        List<MetricsCollectorImpl> collectors = threadValuesHolder.get();
        if (null == collectors) {
            collectors = new ArrayList<>(2);
            threadValuesHolder.set(collectors);
        }
        final MetricsCollectorImpl result = new MetricsCollectorImpl(et, additional, this::collectionFinished);
        collectors.add(result);
        return result;
    }

    public MetricsCollector current() {
        final List<MetricsCollectorImpl> collectors = threadValuesHolder.get();
        if (null == collectors || collectors.isEmpty()) {
            return FAKE_COLLECTOR;
        }
        return collectors.get(collectors.size() - 1);
    }

    public List<MetricsCollectorImpl> poll(final int count) {
        final List<MetricsCollectorImpl> result = new ArrayList<>(count);
        MetricsCollectorImpl item;
        int added = 0;
        while (null != (item = metrics.poll())) {
            result.add(item);
            if (++added >= count) {
                break;
            }
        }
        return result;
    }

    private void collectionFinished(final MetricsCollectorImpl item) {
        final List<MetricsCollectorImpl> items = threadValuesHolder.get();
        if (items.remove(item)) {
            final boolean registeredForTermination = metrics.offer(item);
            if (!registeredForTermination) {
                LOG.warn("Metrics collector is skipping record {}", item);
            }
        } else {
            LOG.error("Bug in code, metric {} is removed several times", item);
        }
    }

    private static final MetricsCollector.Step FAKE_STEP = () -> {
    };

    private static final MetricsCollector FAKE_COLLECTOR = new MetricsCollector() {
        @Override
        public Step start(final String name, final boolean closePrevious) {
            return FAKE_STEP;
        }

        @Override
        public void closeLast() {
        }

        @Override
        public void close() {
        }
    };

}
