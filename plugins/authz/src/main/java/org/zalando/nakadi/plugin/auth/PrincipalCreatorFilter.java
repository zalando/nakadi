package org.zalando.nakadi.plugin.auth;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.oauth2.provider.OAuth2Authentication;
import org.springframework.web.filter.OncePerRequestFilter;
import org.zalando.nakadi.plugin.auth.subject.Principal;
import org.zalando.nakadi.plugin.auth.subject.PrincipalFactory;
import org.zalando.problem.Problem;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.zalando.problem.Status.UNAUTHORIZED;

public class PrincipalCreatorFilter extends OncePerRequestFilter {
    private static final Logger LOGGER = LoggerFactory.getLogger(PrincipalCreatorFilter.class);

    private final PrincipalFactory principalFactory;
    private final ProblemWriter problemWriter;
    private final LoadingCache<PrincipalCacheKey, Principal> cache;

    public PrincipalCreatorFilter(
            final PrincipalFactory principalFactory,
            final ProblemWriter problemWriter,
            final int cacheSize,
            final long cacheExpirationMins) {
        this.principalFactory = principalFactory;
        this.problemWriter = problemWriter;
        this.cache = CacheBuilder.newBuilder()
                .maximumSize(cacheSize)
                .expireAfterWrite(cacheExpirationMins, TimeUnit.MINUTES)
                .build(CacheLoader.from(this::createPrincipal));
    }

    @Override
    protected void doFilterInternal(
            final HttpServletRequest request, final HttpServletResponse response, final FilterChain filterChain)
            throws ServletException, IOException {
        final Authentication authentication = SecurityContextHolder.getContext().getAuthentication();

        if (authentication instanceof OAuth2Authentication) {
            final XConsumer xConsumer = (XConsumer) request.getAttribute(XConsumer.class.getCanonicalName());
            final OAuth2Authentication oAuth2Authentication = (OAuth2Authentication) authentication;

            final String uid = (String) authentication.getPrincipal();
            final String consumer = null == xConsumer ? null : xConsumer.getContent();
            final String realm = getRealm(oAuth2Authentication.getUserAuthentication().getDetails());
            if (null == realm) {
                problemWriter.writeProblem(
                        response,
                        Problem.valueOf(UNAUTHORIZED, "Can not extract realm from token"));
                return;
            }
            try {
                final UsernamePasswordAuthenticationToken token = new UsernamePasswordAuthenticationToken(
                        cache.get(new PrincipalCacheKey(uid, consumer, realm)),
                        null);
                token.setDetails(oAuth2Authentication.getUserAuthentication().getDetails());
                final OAuth2Authentication newOAuth2Authentication = new OAuth2Authentication(
                        oAuth2Authentication.getOAuth2Request(),
                        token
                );
                SecurityContextHolder.getContext().setAuthentication(newOAuth2Authentication);
            } catch (final ExecutionException e) {
                LOGGER.error("Failed to extract token through cache", e);
                problemWriter.writeProblem(response,
                        Problem.valueOf(UNAUTHORIZED, "Token information extraction is failing"));
                return;
            }
        }
        filterChain.doFilter(request, response);
    }

    private Principal createPrincipal(final PrincipalCacheKey key) {
        return principalFactory.createPrincipal(key.uid, key.consumer, key.realm);
    }

    private String getRealm(final Object details) {
        if (details instanceof Map) {
            final Map map = (Map) details;
            final Object realm = map.get("realm");
            if (realm instanceof String) {
                return (String) realm;
            }
        }
        return null;
    }

    private static class PrincipalCacheKey {
        private final String uid;
        private final String consumer;
        private final String realm;

        PrincipalCacheKey(final String uid, final String consumer, final String realm) {
            this.uid = uid;
            this.consumer = consumer;
            this.realm = realm;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final PrincipalCacheKey that = (PrincipalCacheKey) o;
            return Objects.equals(uid, that.uid) &&
                    Objects.equals(consumer, that.consumer) &&
                    Objects.equals(realm, that.realm);
        }

        @Override
        public int hashCode() {
            return Objects.hash(uid, consumer, realm);
        }
    }
}
