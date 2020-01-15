package org.zalando.nakadi.config;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.oauth2.common.OAuth2AccessToken;
import org.springframework.security.oauth2.common.exceptions.OAuth2Exception;
import org.springframework.security.oauth2.provider.OAuth2Authentication;
import org.springframework.security.oauth2.provider.token.ResourceServerTokenServices;
import org.springframework.web.client.RestTemplate;
import org.zalando.nakadi.domain.Feature;
import org.zalando.nakadi.metrics.MetricUtils;
import org.zalando.nakadi.service.FeatureToggleService;
import org.zalando.stups.oauth2.spring.authorization.DefaultUserRolesProvider;
import org.zalando.stups.oauth2.spring.server.DefaultAuthenticationExtractor;
import org.zalando.stups.oauth2.spring.server.TokenInfoResourceServerTokenServices;
import org.zalando.stups.oauth2.spring.server.TokenResponseErrorHandler;

@Configuration
@Profile("!test")
public class AuthenticationConfig {

    @Bean
    public ResourceServerTokenServices zalandoResourceTokenServices(final SecuritySettings settings,
                                                                    final MetricRegistry metricRegistry,
                                                                    final RestTemplate restTemplate,
                                                                    final FeatureToggleService featureToggleService) {
        return new MeasureAndDispatchResourceServerTokenServices(
                metricRegistry, settings, restTemplate, featureToggleService);
    }

    @Bean
    public PoolingHttpClientConnectionManager poolingHttpClientConnectionManager(
            @Value("${nakadi.http.pool.connection.max.total}") final int maxTotal,
            @Value("${nakadi.http.pool.connection.max.per.route}") final int maxPerRoute) {
        final PoolingHttpClientConnectionManager result = new PoolingHttpClientConnectionManager();
        result.setMaxTotal(maxTotal);
        result.setDefaultMaxPerRoute(maxPerRoute);
        return result;
    }

    @Bean
    public RequestConfig requestConfig(
            @Value("${nakadi.http.pool.connection.request.timeout}") final int requestTimeout,
            @Value("${nakadi.http.pool.connection.connect.timeout}") final int connectTimeout,
            @Value("${nakadi.http.pool.connection.socket.timeout}") final int socketTimeout) {
        final RequestConfig result = RequestConfig.custom()
                .setConnectionRequestTimeout(requestTimeout)
                .setConnectTimeout(connectTimeout)
                .setSocketTimeout(socketTimeout)
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
        final RestTemplate restTemplate = new RestTemplate(requestFactory);
        restTemplate.setErrorHandler(TokenResponseErrorHandler.getDefault());
        return restTemplate;
    }

    public static class MeasureAndDispatchResourceServerTokenServices implements ResourceServerTokenServices {

        private final Timer timer;
        private final TokenInfoResourceServerTokenServices remoteService;
        private final TokenInfoResourceServerTokenServices localService;
        private final FeatureToggleService featureToggleService;

        public MeasureAndDispatchResourceServerTokenServices(final MetricRegistry metricRegistry,
                                                             final SecuritySettings securitySettings,
                                                             final RestTemplate restTemplate,
                                                             final FeatureToggleService featureToggleService) {
            remoteService = new TokenInfoResourceServerTokenServices(
                    securitySettings.getTokenInfoUrl(),
                    securitySettings.getClientId(),
                    new DefaultAuthenticationExtractor(),
                    new DefaultUserRolesProvider(),
                    restTemplate);
            localService = new TokenInfoResourceServerTokenServices(
                    securitySettings.getLocalTokenInfoUrl(),
                    securitySettings.getClientId(),
                    new DefaultAuthenticationExtractor(),
                    new DefaultUserRolesProvider(),
                    restTemplate);
            timer = metricRegistry.timer(MetricUtils.NAKADI_PREFIX + "general.accessTokenValidation");
            this.featureToggleService = featureToggleService;
        }

        @Override
        public OAuth2Authentication loadAuthentication(final String accessToken) throws AuthenticationException {
            final Timer.Context context = timer.time();
            try {
                if (featureToggleService.isFeatureEnabled(Feature.REMOTE_TOKENINFO)) {
                    return remoteService.loadAuthentication(accessToken);
                } else {
                    return localService.loadAuthentication(accessToken);
                }
            } catch (final OAuth2Exception e) {
                throw e;
            } catch (final RuntimeException e) {
                throw new OAuth2Exception(e.getMessage(), e);
            } finally {
                context.stop();
            }
        }

        @Override
        public OAuth2AccessToken readAccessToken(final String accessToken) {
            if (featureToggleService.isFeatureEnabled(Feature.REMOTE_TOKENINFO)) {
                return remoteService.readAccessToken(accessToken);
            } else {
                return localService.readAccessToken(accessToken);
            }
        }
    }

}
