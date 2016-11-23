package org.zalando.nakadi.filters;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
public class LoggingFilter extends OncePerRequestFilter {

    private static final Logger LOG = LoggerFactory.getLogger(LoggingFilter.class);

    @Override
    protected void doFilterInternal(final HttpServletRequest request,
                                    final HttpServletResponse response, final FilterChain filterChain)
            throws ServletException, IOException {
        final long start = System.currentTimeMillis();
        final String userAgent = Optional.ofNullable(request.getHeader("User-Agent")).orElse("-");
        final String user = Optional.ofNullable(request.getUserPrincipal()).map(Principal::getName).orElse("-");
        final String method = request.getMethod();
        final String path = request.getRequestURI();
        LOG.info("[ACCESS_LOG_BEGIN] {} \"{}\" \"{}\" \"{}\"",
                method,
                path,
                userAgent,
                user);
        //execute request
        try {
            filterChain.doFilter(request, response);
        } finally {
            final long time = System.currentTimeMillis();
            final Long timing = time - start;
            LOG.info("[ACCESS_LOG_END] {} \"{}\" \"{}\" \"{}\" {} {} ms",
                    method,
                    path,
                    userAgent,
                    user,
                    response.getStatus(),
                    timing);
        }
    }
}
