package de.zalando.aruha.nakadi.config;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import de.zalando.aruha.nakadi.metrics.MetricUtils;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.oauth2.common.exceptions.InvalidTokenException;
import org.springframework.security.oauth2.provider.OAuth2Authentication;
import org.springframework.security.oauth2.provider.token.ResourceServerTokenServices;
import org.zalando.stups.oauth2.spring.server.TokenInfoResourceServerTokenServices;

import static de.zalando.aruha.nakadi.config.NakadiConfig.METRIC_REGISTRY;

@Configuration
@Profile("!test")
public class AuthenticationConfig {

    @Bean
    public ResourceServerTokenServices zalandoResourceTokenServices() {
        return new MeasuringTokenInfoResourceServerTokenServices(
                securitySettings().getTokenInfoUrl(), securitySettings().getClientId(), METRIC_REGISTRY);
    }

    @Bean
    public SecuritySettings securitySettings() {
        return new SecuritySettings();
    }

    public static class MeasuringTokenInfoResourceServerTokenServices extends TokenInfoResourceServerTokenServices {
        private final Timer timer;

        public MeasuringTokenInfoResourceServerTokenServices(final String tokenInfoEndpointUrl, final String clientId, final MetricRegistry metricRegistry) {
            super(tokenInfoEndpointUrl, clientId);
            timer = metricRegistry.timer(MetricUtils.NAKADI_PREFIX + "general.accessTokenValidation");
        }

        @Override
        public OAuth2Authentication loadAuthentication(final String accessToken) throws AuthenticationException, InvalidTokenException {
            final Timer.Context context = timer.time();
            try {
                return super.loadAuthentication(accessToken);
            } finally {
                context.stop();
            }
        }
    }
}
