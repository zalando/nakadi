package org.zalando.nakadi.config;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.oauth2.common.OAuth2AccessToken;
import org.springframework.security.oauth2.common.exceptions.OAuth2Exception;
import org.springframework.security.oauth2.provider.OAuth2Authentication;
import org.springframework.security.oauth2.provider.token.ResourceServerTokenServices;
import org.zalando.nakadi.domain.Feature;
import org.zalando.nakadi.metrics.MetricUtils;
import org.zalando.nakadi.service.FeatureToggleService;
import org.zalando.stups.oauth2.spring.server.TokenInfoResourceServerTokenServices;

public class NakadiResourceServerTokenServices implements ResourceServerTokenServices {

    private final Timer timer;
    private final TokenInfoResourceServerTokenServices remoteService;
    private final TokenInfoResourceServerTokenServices localService;
    private final FeatureToggleService featureToggleService;
    private static final Logger LOG = LoggerFactory.getLogger(NakadiResourceServerTokenServices.class);

    public NakadiResourceServerTokenServices(
            final MetricRegistry metricRegistry,
            final TokenInfoResourceServerTokenServices localService,
            final TokenInfoResourceServerTokenServices remoteService,
            final FeatureToggleService featureToggleService) {
        this.remoteService = remoteService;
        this.localService = localService;
        this.timer = metricRegistry.timer(MetricUtils.NAKADI_PREFIX + "general.accessTokenValidation");
        this.featureToggleService = featureToggleService;
    }

    @Override
    public OAuth2Authentication loadAuthentication(final String accessToken) throws AuthenticationException {
        final Timer.Context context = timer.time();
        try {
            // Try local, if exception is unexpected - try remote.
            if (!featureToggleService.isFeatureEnabled(Feature.REMOTE_TOKENINFO)) {
                try {
                    return localService.loadAuthentication(accessToken);
                } catch (final OAuth2Exception ex) {
                    throw ex;
                } catch (RuntimeException ex) {
                    // This event should be pretty rare, so it is fine to log this exception.
                    LOG.error("Failed to load local tokeninfo information", ex);
                }
            }
            // local din't work, let's try remote.
            return remoteService.loadAuthentication(accessToken);
        } catch (final OAuth2Exception e) {
            throw e;
        } catch (final RuntimeException e) {
            LOG.error("Failed to load remote tokeninfo", e); // this is the problem.
            // The reason why we need to throw OAuth2Exception is the way how security is handled
            throw new OAuth2Exception(e.getMessage(), e) {
                @Override
                public int getHttpErrorCode() {
                    return HttpStatus.SC_SERVICE_UNAVAILABLE;
                }
            };
        } finally {
            context.stop();
        }
    }

    // This anyway is unsupported, will not change it.
    @Override
    public OAuth2AccessToken readAccessToken(final String accessToken) {
        if (featureToggleService.isFeatureEnabled(Feature.REMOTE_TOKENINFO)) {
            return remoteService.readAccessToken(accessToken);
        } else {
            return localService.readAccessToken(accessToken);
        }
    }
}
