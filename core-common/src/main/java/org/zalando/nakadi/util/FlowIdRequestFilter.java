package org.zalando.nakadi.util;

import org.apache.commons.lang3.RandomStringUtils;

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
    }

    @Override
    public void doFilter(final ServletRequest request, final ServletResponse response, final FilterChain chain)
            throws IOException, ServletException {
        final String flowId = getFlowIdFromRequestOrGenerate(request);

        if (response instanceof HttpServletResponse) {
            final HttpServletResponse httpServletResponse = (HttpServletResponse) response;
            httpServletResponse.setHeader(X_FLOW_ID_HEADER, flowId);
        }

        try (MDCUtils.CloseableNoEx ignored = MDCUtils.withFlowId(flowId)) {
            chain.doFilter(request, response);
        }
    }

    private String getFlowIdFromRequestOrGenerate(final ServletRequest request) {
        if (request instanceof HttpServletRequest) {
            final HttpServletRequest httpServletRequest = (HttpServletRequest) request;
            final String requestIdFromRequest = httpServletRequest.getHeader(X_FLOW_ID_HEADER);
            if (null != requestIdFromRequest) {
                return requestIdFromRequest;
            }
        }
        return RandomStringUtils.randomAlphanumeric(24);
    }

    @Override
    public void destroy() {
    }
}
