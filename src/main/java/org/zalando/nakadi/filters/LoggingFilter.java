package org.zalando.nakadi.filters;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.oauth2.provider.OAuth2Authentication;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.security.Principal;
import java.util.Map;
import java.util.Optional;

@Component
public class LoggingFilter extends OncePerRequestFilter {

    private static final Logger LOG = LoggerFactory.getLogger(LoggingFilter.class);

    @Override
    protected void doFilterInternal(final HttpServletRequest request,
                                    final HttpServletResponse response, final FilterChain filterChain)
            throws ServletException, IOException {
        final long start = System.currentTimeMillis();
        try {
            //execute request
            filterChain.doFilter(request, response);
        } finally {
            final long time = System.currentTimeMillis();
            final Long timing = time - start;
            final String userAgent = Optional.ofNullable(request.getHeader("User-Agent")).orElse("-");
            final String user = Optional.ofNullable(request.getUserPrincipal()).map(Principal::getName).orElse("-");
            final String method = request.getMethod();
            final String path = request.getRequestURI();
            final String query = Optional.ofNullable(request.getQueryString()).map(q -> "?" + q).orElse("");

            LOG.info("[ACCESS_LOG] {} \"{}{}\" \"{}\" \"{}\" statusCode: {} {} ms",
                    method,
                    path,
                    query,
                    userAgent,
                    user,
                    response.getStatus(),
                    timing);

            // todo: delete after we collect the information we need
            // this is done as a separate log entry not to break the log parser we have for our access log
            LOG.info("[REALM] \"{}\" \"{}\"", getRealm().orElse("-"), user);
        }
    }

    private Optional<String> getRealm() {
        final Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        if (authentication instanceof OAuth2Authentication) {
            Object details = ((OAuth2Authentication) authentication).getUserAuthentication().getDetails();
            if (details instanceof Map) {
                Map map = (Map) details;
                Object realm = map.get("realm");
                if (realm != null && realm instanceof String) {
                    return Optional.of((String) realm);
                }
            }
        }
        return Optional.empty();
    }
}
