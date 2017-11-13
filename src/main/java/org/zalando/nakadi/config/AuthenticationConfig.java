package org.zalando.nakadi.config;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.oauth2.provider.OAuth2Authentication;
import org.springframework.security.oauth2.provider.token.ResourceServerTokenServices;
import org.springframework.web.client.RestTemplate;
import org.zalando.nakadi.metrics.MetricUtils;
import org.zalando.stups.oauth2.spring.authorization.DefaultUserRolesProvider;
import org.zalando.stups.oauth2.spring.server.DefaultAuthenticationExtractor;
import org.zalando.stups.oauth2.spring.server.TokenInfoResourceServerTokenServices;

@Configuration
@Profile("!test")
public class AuthenticationConfig {

    @Bean
    public ResourceServerTokenServices zalandoResourceTokenServices(final SecuritySettings settings,
                                                                    final MetricRegistry metricRegistry,
                                                                    final RestTemplate restTemplate) {
        return new MeasuringTokenInfoResourceServerTokenServices(
                settings.getTokenInfoUrl(), settings.getClientId(), metricRegistry, restTemplate);
    }

    public static class MeasuringTokenInfoResourceServerTokenServices extends TokenInfoResourceServerTokenServices {
        private final Timer timer;

        public MeasuringTokenInfoResourceServerTokenServices(final String tokenInfoEndpointUrl,
                                                             final String clientId,
                                                             final MetricRegistry metricRegistry,
                                                             final RestTemplate restTemplate) {
            super(tokenInfoEndpointUrl,
                    clientId,
                    new DefaultAuthenticationExtractor(),
                    new DefaultUserRolesProvider(),
                    restTemplate);
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

    @Bean
    public PoolingHttpClientConnectionManager poolingHttpClientConnectionManager() {
        final PoolingHttpClientConnectionManager result = new PoolingHttpClientConnectionManager();
        result.setMaxTotal(20);
        result.setDefaultMaxPerRoute(20);
        return result;
    }

    @Bean
    public RequestConfig requestConfig() {
        final RequestConfig result = RequestConfig.custom()
                .setConnectionRequestTimeout(500)
                .setConnectTimeout(1000)
                .setSocketTimeout(2000)
                .build();
        return result;
    }

    @Bean
    public CloseableHttpClient httpClient(final PoolingHttpClientConnectionManager poolingHttpClientConnectionManager,
                                          final RequestConfig requestConfig) {
        final CloseableHttpClient result = HttpClientBuilder
                .create()
                .setConnectionManager(poolingHttpClientConnectionManager)
                .setDefaultRequestConfig(requestConfig)
                .build();
        return result;
    }

    @Bean
    public RestTemplate restTemplate(final HttpClient httpClient) {
        final HttpComponentsClientHttpRequestFactory requestFactory = new HttpComponentsClientHttpRequestFactory();
        requestFactory.setHttpClient(httpClient);
        return new RestTemplate(requestFactory);
    }
}
