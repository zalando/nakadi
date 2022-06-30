package org.zalando.nakadi.filters;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import org.springframework.web.filter.OncePerRequestFilter;
import org.zalando.nakadi.config.SecuritySettings;
import org.zalando.nakadi.metrics.MetricUtils;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.plugin.api.authz.Subject;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

public class MonitoringRequestFilter extends OncePerRequestFilter {

    private final Timer httpConnectionsTimer;
    private final Counter openHttpConnectionsCounter;
    private final MetricRegistry perPathMetricRegistry;
    private final AuthorizationService authorizationService;

    public MonitoringRequestFilter(final MetricRegistry metricRegistry,
                                   final MetricRegistry perPathMetricRegistry,
                                   final AuthorizationService authorizationService) {
        openHttpConnectionsCounter = metricRegistry.counter(MetricUtils.NAKADI_PREFIX
                + "general.openSynchronousHttpConnections");
        httpConnectionsTimer = metricRegistry
                .timer(MetricUtils.NAKADI_PREFIX + "general.synchronousHttpConnections");
        this.perPathMetricRegistry = perPathMetricRegistry;
        this.authorizationService = authorizationService;
    }

    @Override
    protected void doFilterInternal(final HttpServletRequest request,
                                    final HttpServletResponse response, final FilterChain filterChain)
            throws IOException, ServletException {
        openHttpConnectionsCounter.inc();
        final Timer.Context timerContext = httpConnectionsTimer.time();

        final String clientId = authorizationService.getSubject().map(Subject::getName)
                .orElse(SecuritySettings.UNAUTHENTICATED_CLIENT_ID);
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
