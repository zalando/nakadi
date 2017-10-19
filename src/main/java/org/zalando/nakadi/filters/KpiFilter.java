package org.zalando.nakadi.filters;

import org.springframework.web.filter.OncePerRequestFilter;
import org.zalando.nakadi.metrics.MetricsCollector;
import org.zalando.nakadi.metrics.NakadiKPIMetrics;
import org.zalando.nakadi.util.FlowIdUtils;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class KpiFilter extends OncePerRequestFilter {
    private final NakadiKPIMetrics kpiMetrics;

    public KpiFilter(final NakadiKPIMetrics kpiMetrics) {
        this.kpiMetrics = kpiMetrics;
    }

    @Override
    protected void doFilterInternal(
            final HttpServletRequest request,
            final HttpServletResponse response,
            final FilterChain filterChain)
            throws ServletException, IOException {
        final Map<String, String> info = new HashMap<>();
        info.put("url", request.getRequestURI());
        info.put("method", request.getMethod());
        info.put("flow_id", FlowIdUtils.peek());
        final MetricsCollector collector = kpiMetrics.startCollection("nakadi.kpi_metrics", info);
        try {
            filterChain.doFilter(request, response);
        } finally {
            collector.close();
        }
    }
}
