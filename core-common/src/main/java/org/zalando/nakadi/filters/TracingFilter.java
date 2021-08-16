package org.zalando.nakadi.filters;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.plugin.api.authz.Subject;
import org.zalando.nakadi.service.TracingService;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

@Component
public class TracingFilter extends OncePerRequestFilter {

    private final AuthorizationService authorizationService;

    @Autowired
    public TracingFilter(final AuthorizationService authorizationService) {
        this.authorizationService = authorizationService;
    }

    @Override
    protected void doFilterInternal(final HttpServletRequest request,
                                    final HttpServletResponse response,
                                    final FilterChain filterChain)
            throws IOException, ServletException {

        TracingService.setTag("client_id", authorizationService.getSubject().map(Subject::getName).orElse("-"));

        filterChain.doFilter(request, response);
    }
}
