package org.zalando.nakadi.config;

import com.codahale.metrics.MetricRegistry;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.security.oauth2.provider.token.ResourceServerTokenServices;
import org.springframework.web.client.RestTemplate;
import org.zalando.nakadi.service.FeatureToggleService;
import org.zalando.stups.oauth2.spring.authorization.DefaultUserRolesProvider;
import org.zalando.stups.oauth2.spring.server.DefaultAuthenticationExtractor;
import org.zalando.stups.oauth2.spring.server.TokenInfoResourceServerTokenServices;
import org.zalando.stups.oauth2.spring.server.TokenResponseErrorHandler;

@Configuration
@Profile("!test")
public class AuthenticationConfig {

    @Bean
    @Qualifier("remote")
    public TokenInfoResourceServerTokenServices remoteTokenInfo(
            final RestTemplate restTemplate,
            final SecuritySettings securitySettings) {
        return new TokenInfoResourceServerTokenServices(
                securitySettings.getTokenInfoUrl(),
                securitySettings.getClientId(),
                new DefaultAuthenticationExtractor(),
                new DefaultUserRolesProvider(),
                restTemplate
        );
    }

    @Bean
    @Qualifier("local")
    public TokenInfoResourceServerTokenServices localTokenInfo(
            final RestTemplate restTemplate,
            final SecuritySettings securitySettings) {
        return new TokenInfoResourceServerTokenServices(
                securitySettings.getLocalTokenInfoUrl(),
                securitySettings.getClientId(),
                new DefaultAuthenticationExtractor(),
                new DefaultUserRolesProvider(),
                restTemplate
        );
    }


    @Bean
    @Primary
    public ResourceServerTokenServices zalandoResourceTokenServices(
            @Qualifier("remote") final TokenInfoResourceServerTokenServices remoteTokenInfo,
            @Qualifier("local") final TokenInfoResourceServerTokenServices localTokenInfo,
            final MetricRegistry metricRegistry,
            final FeatureToggleService featureToggleService) {
        return new NakadiResourceServerTokenServices(
                metricRegistry, localTokenInfo, remoteTokenInfo, featureToggleService);
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

}
