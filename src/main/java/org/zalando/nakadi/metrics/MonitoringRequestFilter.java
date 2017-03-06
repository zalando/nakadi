package org.zalando.nakadi.metrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import java.io.IOException;

public class MonitoringRequestFilter implements Filter {

    private final Timer httpConnectionsTimer;
    private final Counter openHttpConnectionsCounter;
    private final MetricRegistry perPathMetricRegistry;

    public MonitoringRequestFilter(final MetricRegistry overallMetricRegistry,
                                   final MetricRegistry perPathMetricRegistry) {
        openHttpConnectionsCounter = overallMetricRegistry.counter(MetricUtils.NAKADI_PREFIX
                + "general.openSynchronousHttpConnections");
        httpConnectionsTimer = overallMetricRegistry
                .timer(MetricUtils.NAKADI_PREFIX + "general.synchronousHttpConnections");
        this.perPathMetricRegistry = perPathMetricRegistry;
    }

    @Override
    public void init(final FilterConfig filterConfig) throws ServletException {
        // nothing to do
    }

    @Override
    public void doFilter(final ServletRequest request, final ServletResponse response, final FilterChain chain)
            throws IOException, ServletException {
        openHttpConnectionsCounter.inc();
        final Timer.Context timerContext = httpConnectionsTimer.time();

        Timer.Context perPathTimerContext = null;
        if (request instanceof HttpServletRequest) {
            final HttpServletRequest httpRequest = (HttpServletRequest) request;
            final String perPathMetricKey = httpRequest.getMethod() + httpRequest.getServletPath();
            perPathTimerContext = perPathMetricRegistry.timer(perPathMetricKey).time();
        }

        try {

            chain.doFilter(request, response);

        } finally {
            if (perPathTimerContext != null) {
                perPathTimerContext.stop();
            }
            timerContext.stop();
            openHttpConnectionsCounter.dec();
        }

    }

    @Override
    public void destroy() {
        // nothing to do
    }
}
