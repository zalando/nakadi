package org.zalando.nakadi.filters;

import org.springframework.security.web.firewall.RequestRejectedException;
import org.springframework.web.filter.GenericFilterBean;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

public class RequestRejectedFilter extends GenericFilterBean {

    @Override
    public void doFilter(final ServletRequest req,
                         final ServletResponse res,
                         final FilterChain chain)
            throws IOException, ServletException {
        try {
            chain.doFilter(req, res);
        } catch (RequestRejectedException e) {
            final HttpServletResponse response = (HttpServletResponse) res;

            response.sendError(HttpServletResponse.SC_BAD_REQUEST);
        }
    }
}
