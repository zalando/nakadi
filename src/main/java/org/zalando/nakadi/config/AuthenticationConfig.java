package org.zalando.nakadi.config;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.oauth2.provider.OAuth2Authentication;
import org.springframework.security.oauth2.provider.token.ResourceServerTokenServices;
import org.zalando.nakadi.metrics.MetricUtils;
import org.zalando.stups.oauth2.spring.server.TokenInfoResourceServerTokenServices;

@Configuration
@Profile("!test")
public class AuthenticationConfig {

    @Bean
    public ResourceServerTokenServices zalandoResourceTokenServices(final SecuritySettings settings,
                                                                    final MetricRegistry metricRegistry) {
        return new MeasuringTokenInfoResourceServerTokenServices(
                settings.getTokenInfoUrl(), settings.getClientId(), metricRegistry);
    }

    public static class MeasuringTokenInfoResourceServerTokenServices extends TokenInfoResourceServerTokenServices {
        private final Timer timer;

        public MeasuringTokenInfoResourceServerTokenServices(final String tokenInfoEndpointUrl, final String clientId,
                                                             final MetricRegistry metricRegistry) {
            super(tokenInfoEndpointUrl, clientId);
            timer = metricRegistry.timer(MetricUtils.NAKADI_PREFIX + "general.accessTokenValidation");
        }

        @Override
        public OAuth2Authentication loadAuthentication(final String accessToken) throws AuthenticationException {
            final Timer.Context context = timer.time();
            try {
                return super.loadAuthentication(accessToken);
            } finally {
                context.stop();
            }
        }
    }
}
