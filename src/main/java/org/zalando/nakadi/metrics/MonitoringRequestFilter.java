package org.zalando.nakadi.metrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.security.Principal;
import java.util.Optional;

@Component
public class MonitoringRequestFilter extends OncePerRequestFilter {

    private final Timer httpConnectionsTimer;
    private final Counter openHttpConnectionsCounter;
    private final MetricRegistry perPathMetricRegistry;

    @Autowired
    public MonitoringRequestFilter(final MetricRegistry metricRegistry,
                                   @Qualifier("perPathMetricRegistry") final MetricRegistry perPathMetricRegistry) {
        openHttpConnectionsCounter = metricRegistry.counter(MetricUtils.NAKADI_PREFIX
                + "general.openSynchronousHttpConnections");
        httpConnectionsTimer = metricRegistry
                .timer(MetricUtils.NAKADI_PREFIX + "general.synchronousHttpConnections");
        this.perPathMetricRegistry = perPathMetricRegistry;
    }

    @Override
    protected void doFilterInternal(final HttpServletRequest request,
                                    final HttpServletResponse response, final FilterChain filterChain)
            throws IOException, ServletException {
        openHttpConnectionsCounter.inc();
        final Timer.Context timerContext = httpConnectionsTimer.time();

        final String clientId = Optional.ofNullable(request.getUserPrincipal())
                .map(Principal::getName)
                .orElse("unauthenticated");
        final String perPathMetricKey = MetricRegistry.name(
                clientId,
                request.getMethod(),
                request.getServletPath());
        final Timer.Context perPathTimerContext = perPathMetricRegistry.timer(perPathMetricKey).time();

        try {
            filterChain.doFilter(request, response);
        } finally {
            perPathTimerContext.stop();
            timerContext.stop();
            openHttpConnectionsCounter.dec();
        }

    }
}
