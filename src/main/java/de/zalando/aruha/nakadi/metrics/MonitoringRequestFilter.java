package de.zalando.aruha.nakadi.metrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import java.io.IOException;

public class MonitoringRequestFilter implements Filter {

    private final Timer httpConnectionsTimer;
    private final Counter openHttpConnectionsCounter;

    public MonitoringRequestFilter(final MetricRegistry metricRegistry) {
        openHttpConnectionsCounter = metricRegistry.counter(MetricUtils.NAKADI_PREFIX + "general.openSynchronousHttpConnections");
        httpConnectionsTimer = metricRegistry.timer(MetricUtils.NAKADI_PREFIX + "general.synchronousHttpConnections");
    }

    @Override
    public void init(final FilterConfig filterConfig) throws ServletException {
        // nothing to do
    }

    @Override
    public void doFilter(final ServletRequest request, final ServletResponse response, final FilterChain chain) throws IOException, ServletException {
        openHttpConnectionsCounter.inc();
        final Timer.Context timerContext = httpConnectionsTimer.time();

        try {

            chain.doFilter(request, response);

        } finally {
            timerContext.stop();
            openHttpConnectionsCounter.dec();
        }

    }

    @Override
    public void destroy() {
        // nothing to do
    }
}
