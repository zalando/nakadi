package org.zalando.nakadi.config;

import com.codahale.metrics.MetricRegistry;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.RequestEntity;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.security.oauth2.provider.token.ResourceServerTokenServices;
import org.springframework.util.Assert;
import org.springframework.web.client.HttpStatusCodeException;
import org.springframework.web.client.RestTemplate;
import org.zalando.nakadi.exceptions.runtime.ServiceTemporarilyUnavailableException;
import org.zalando.nakadi.service.FeatureToggleService;
import org.zalando.stups.oauth2.spring.authorization.DefaultUserRolesProvider;
import org.zalando.stups.oauth2.spring.server.DefaultAuthenticationExtractor;
import org.zalando.stups.oauth2.spring.server.TokenInfoRequestExecutor;
import org.zalando.stups.oauth2.spring.server.TokenInfoResourceServerTokenServices;
import org.zalando.stups.oauth2.spring.server.TokenResponseErrorHandler;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.Collections;
import java.util.Map;


@Configuration
@Profile("!test")
public class AuthenticationConfig {

    @Bean
    @Qualifier("remote")
    public TokenInfoResourceServerTokenServices remoteTokenInfo(
            final RestTemplate restTemplate,
            final SecuritySettings securitySettings) {
        return new TokenInfoResourceServerTokenServices(
                securitySettings.getClientId(),
                new DefaultAuthenticationExtractor(),
                new DefaultUserRolesProvider(),
                new NakadiTokenInfoRequestExecutor(securitySettings.getTokenInfoUrl(), restTemplate)
        );
    }

    @Bean
    @Qualifier("local")
    public TokenInfoResourceServerTokenServices localTokenInfo(
            final RestTemplate restTemplate,
            final SecuritySettings securitySettings) {
        return new TokenInfoResourceServerTokenServices(
                securitySettings.getClientId(),
                new DefaultAuthenticationExtractor(),
                new DefaultUserRolesProvider(),
                new NakadiTokenInfoRequestExecutor(securitySettings.getLocalTokenInfoUrl(), restTemplate)
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

    public static class NakadiTokenInfoRequestExecutor implements TokenInfoRequestExecutor {
        private static final Logger LOGGER = LoggerFactory.getLogger(NakadiTokenInfoRequestExecutor.class);
        private static final String BEARER_TOKEN_TEMPLATE = "Bearer %s";
        private static final ParameterizedTypeReference<Map<String, Object>> TOKENINFO_MAP =
                new ParameterizedTypeReference<>() {
                };

        private final URI tokenInfoURL;
        private final RestTemplate restTemplate;

        public NakadiTokenInfoRequestExecutor(final String tokenInfoURL, final RestTemplate restTemplate) {
            this.restTemplate = restTemplate;
            Assert.notNull(restTemplate, "'restTemplate' should never be null");
            Assert.hasText(tokenInfoURL, "TokenInfoEndpointUrl should never be null or empty");
            try {
                new URL(tokenInfoURL);
            } catch (MalformedURLException e) {
                throw new IllegalArgumentException("TokenInfoEndpointUrl is not an URL", e);
            }

            this.tokenInfoURL = URI.create(tokenInfoURL);
        }

        @Override
        public Map<String, Object> getMap(final String accessToken) {
            LOGGER.debug("Getting token-info from: {}", tokenInfoURL);

            final RequestEntity<Void> entity = RequestEntity.get(tokenInfoURL).accept(MediaType.APPLICATION_JSON)
                    .header(HttpHeaders.AUTHORIZATION, String.format(BEARER_TOKEN_TEMPLATE, accessToken)).build();
            try {
                return restTemplate.exchange(entity, TOKENINFO_MAP).getBody();
            } catch (HttpStatusCodeException e) {
                throw new ServiceTemporarilyUnavailableException("Unable to validate token", e);
            } catch (Exception e) {
                LOGGER.warn("Unable to get authorisation from tokeninfo", e);
                return Collections.singletonMap("error", e.getMessage());
            }
        }
    }

}
