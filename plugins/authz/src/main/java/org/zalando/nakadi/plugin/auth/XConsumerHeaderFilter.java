package org.zalando.nakadi.plugin.auth;

import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.oauth2.provider.OAuth2Authentication;
import org.springframework.security.web.util.matcher.AntPathRequestMatcher;
import org.springframework.web.filter.OncePerRequestFilter;
import org.zalando.problem.Problem;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Optional;

import static org.zalando.problem.Status.BAD_REQUEST;
import static org.zalando.problem.Status.UNAUTHORIZED;

public class XConsumerHeaderFilter extends OncePerRequestFilter {

    private final MerchantGatewayService merchantGatewayService;
    private final List<AntPathRequestMatcher> merchantOpenEndpoints;
    private final List<String> merchantUids;
    private final String merchantScope;
    private final ProblemWriter problemWriter;

    private static final Problem SIGNATURE_PROBLEM = Problem
            .valueOf(UNAUTHORIZED, "Problem verifying request origin");
    private static final Problem CONSUMER_INVALID_PROBLEM = Problem
            .valueOf(BAD_REQUEST, "Invalid request origin parameters");
    private static final Problem INSUFFICIENT_SCOPES_PROBLEM = Problem
            .valueOf(UNAUTHORIZED, "Insufficient scopes for this operation");
    private static final Problem ACCESS_NOT_ALLOWED_THROUGH_MERCHANT_GATEWAY = Problem.
            valueOf(UNAUTHORIZED, "Access to this endpoint is not allowed through Merchant Gateway");
    
    public XConsumerHeaderFilter(final MerchantGatewayService merchantGatewayService,
                                 final List<String> merchantUids,
                                 final String merchantScope,
                                 final ProblemWriter problemWriter) {
        this.merchantGatewayService = merchantGatewayService;
        this.merchantUids = merchantUids;
        this.merchantScope = merchantScope;
        this.problemWriter = problemWriter;
        merchantOpenEndpoints = new ArrayList<AntPathRequestMatcher>() {{
            add(new AntPathRequestMatcher("/health", "GET"));
            add(new AntPathRequestMatcher("/event-types/*", "GET"));
            add(new AntPathRequestMatcher("/event-types/*/cursor-distances", "POST"));
            add(new AntPathRequestMatcher("/event-types/*/cursors-lag", "POST"));
            add(new AntPathRequestMatcher("/event-types/*/partitions", "GET"));
            add(new AntPathRequestMatcher("/event-types/*/partitions/*", "GET"));
            add(new AntPathRequestMatcher("/event-types/*/schemas", "GET"));
            add(new AntPathRequestMatcher("/event-types/*/schemas/*", "GET"));
            add(new AntPathRequestMatcher("/event-types/*/shifted-cursors", "POST"));
            add(new AntPathRequestMatcher("/subscriptions", "POST"));
            add(new AntPathRequestMatcher("/subscriptions/*", "GET"));
            add(new AntPathRequestMatcher("/subscriptions/*/cursors", "GET"));
            add(new AntPathRequestMatcher("/subscriptions/*/cursors", "POST"));
            add(new AntPathRequestMatcher("/subscriptions/*/cursors", "PATCH"));
            add(new AntPathRequestMatcher("/subscriptions/*/events", "GET"));
            add(new AntPathRequestMatcher("/subscriptions/*/events", "POST"));
            add(new AntPathRequestMatcher("/subscriptions/*/stats", "GET"));
        }};
    }

    @Override
    protected void doFilterInternal(final HttpServletRequest request, final HttpServletResponse response,
                                    final FilterChain filterChain) throws ServletException, IOException {

        final Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        final XConsumer xConsumer = XConsumer.fromRequest(request);

        if (xConsumer.isPresent()) {
            if (!merchantGatewayService.isValidJsonWhenDecoded(xConsumer)) {
                problemWriter.writeProblem(response, CONSUMER_INVALID_PROBLEM);
                return;
            } else if (!merchantGatewayService.isSignatureValid(xConsumer)) {
                problemWriter.writeProblem(response, SIGNATURE_PROBLEM);
                return;
            } else if (!xConsumerHasRequiredScope(xConsumer.getContent())) {
                problemWriter.writeProblem(response, INSUFFICIENT_SCOPES_PROBLEM);
                return;
            }
            request.setAttribute(XConsumer.class.getCanonicalName(), xConsumer);
        }

        if (authentication instanceof OAuth2Authentication) {
            final String uid = (String) authentication.getPrincipal();
            if (merchantUids.contains(uid) && !isGatewayAllowedToAccessEndpoint(request)) {
                problemWriter.writeProblem(response, ACCESS_NOT_ALLOWED_THROUGH_MERCHANT_GATEWAY);
                return;
            }
        }
        filterChain.doFilter(request, response);
    }

    private boolean xConsumerHasRequiredScope(final String content) {
        final JSONObject bussinessPartnerJson = new JSONObject(
                new String(Base64.getDecoder().decode(content)));
        final JSONArray scopesArray = Optional.ofNullable(bussinessPartnerJson
                .optJSONArray("scopes"))
                .orElse(new JSONArray());
        return scopesArray.toList().stream()
                .map(scope -> (String) scope).anyMatch(p -> p.equals(merchantScope));
    }

    private boolean isGatewayAllowedToAccessEndpoint(final HttpServletRequest request) {
        for (final AntPathRequestMatcher matcher : merchantOpenEndpoints) {
            if (matcher.matches(request)) {
                return true;
            }
        }
        return false;
    }
}
