package org.zalando.nakadi.util;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

public class FlowIdRequestFilter implements Filter {
    public static final String X_FLOW_ID_HEADER = "X-Flow-Id";

    @Override
    public void init(final FilterConfig filterConfig) throws ServletException {
        // This constructor is intentionally empty, because something something
    }

    @Override
    public void doFilter(final ServletRequest request, final ServletResponse response, final FilterChain chain)
            throws IOException, ServletException {
        FlowIdUtils.clear();
        String flowId = null;

        if (request instanceof HttpServletRequest) {
            final HttpServletRequest httpServletRequest = (HttpServletRequest) request;
            flowId = httpServletRequest.getHeader(X_FLOW_ID_HEADER);
        }

        if (flowId == null) {
            flowId = FlowIdUtils.generateFlowId();
        }

        FlowIdUtils.push(flowId);

        if (response instanceof HttpServletResponse) {
            final HttpServletResponse httpServletResponse = (HttpServletResponse) response;
            httpServletResponse.setHeader(X_FLOW_ID_HEADER, flowId);
        }

        try {
            chain.doFilter(request, response);

        } finally {
            FlowIdUtils.clear();
        }
    }

    @Override
    public void destroy() {
        // This constructor is intentionally empty, because something something
    }
}
