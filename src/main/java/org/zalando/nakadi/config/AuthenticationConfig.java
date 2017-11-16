package org.zalando.nakadi.config;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.MediaType;
import org.springframework.http.RequestEntity;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.oauth2.provider.OAuth2Authentication;
import org.springframework.security.oauth2.provider.token.ResourceServerTokenServices;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;
import org.zalando.nakadi.metrics.MetricUtils;
import org.zalando.nakadi.util.FeatureToggleService;
import org.zalando.stups.oauth2.spring.authorization.DefaultUserRolesProvider;
import org.zalando.stups.oauth2.spring.server.DefaultAuthenticationExtractor;
import org.zalando.stups.oauth2.spring.server.TokenInfoRequestExecutor;
import org.zalando.stups.oauth2.spring.server.TokenInfoResourceServerTokenServices;
import org.zalando.stups.oauth2.spring.server.TokenResponseErrorHandler;

import java.net.URI;
import java.util.Map;

import static org.springframework.http.HttpHeaders.AUTHORIZATION;
import static org.springframework.security.oauth2.common.OAuth2AccessToken.BEARER_TYPE;

@Configuration
@Profile("!test")
public class AuthenticationConfig {

    @Bean
    public ResourceServerTokenServices zalandoResourceTokenServices(final SecuritySettings settings,
                                                                    final MetricRegistry metricRegistry,
                                                                    final RestTemplate restTemplate,
                                                                    final FeatureToggleService featureToggleService) {
        final SplitTokenInfoExecutor executor =
                new SplitTokenInfoExecutor(featureToggleService, settings, restTemplate);
        return new MeasuringTokenInfoResourceServerTokenServices(settings.getClientId(), metricRegistry, executor);
    }

    @Bean
    public PoolingHttpClientConnectionManager poolingHttpClientConnectionManager() {
        final PoolingHttpClientConnectionManager result = new PoolingHttpClientConnectionManager();
        result.setMaxTotal(20);
        result.setDefaultMaxPerRoute(10);
        return result;
    }

    @Bean
    public RequestConfig requestConfig() {
        final RequestConfig result = RequestConfig.custom()
                .setConnectionRequestTimeout(2000)
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
        final RestTemplate restTemplate = new RestTemplate(requestFactory);
        restTemplate.setErrorHandler(TokenResponseErrorHandler.getDefault());
        return restTemplate;
    }

    public static class MeasuringTokenInfoResourceServerTokenServices extends TokenInfoResourceServerTokenServices {
        private final Timer timer;

        public MeasuringTokenInfoResourceServerTokenServices(final String clientId,
                                                             final MetricRegistry metricRegistry,
                                                             final SplitTokenInfoExecutor splitTokenInfoExecutor) {
            super(clientId,
                    new DefaultAuthenticationExtractor(),
                    new DefaultUserRolesProvider(),
                    splitTokenInfoExecutor);
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

    @Component
    private static class SplitTokenInfoExecutor implements TokenInfoRequestExecutor {

        private static final Logger LOG = LoggerFactory.getLogger(SplitTokenInfoExecutor.class);
        private static final String SPACE = " ";
        private static final ParameterizedTypeReference<Map<String, Object>> TOKENINFO_MAP =
                new ParameterizedTypeReference<Map<String, Object>>() { };

        private final FeatureToggleService featureToggleService;
        private final RestTemplate restTemplate;
        private final URI remoteTokenInfoEndpointUri;
        private final URI localTokenInfoEndpointUri;

        @Autowired
        public SplitTokenInfoExecutor(final FeatureToggleService featureToggleService,
                                      final SecuritySettings settings,
                                      final RestTemplate restTemplate) {
            this.featureToggleService = featureToggleService;
            this.restTemplate = restTemplate;
            this.remoteTokenInfoEndpointUri = URI.create(settings.getTokenInfoUrl());
            this.localTokenInfoEndpointUri = URI.create(settings.getLocalTokenInfoUrl());
        }

        @Override
        public Map<String, Object> getMap(final String accessToken) {
            return doGetMap(accessToken);
        }

        protected Map<String, Object> doGetMap(final String accessToken) {
            if (featureToggleService.isFeatureEnabled(FeatureToggleService.Feature.REMOTE_TOKENINFO)) {
                return call(remoteTokenInfoEndpointUri, accessToken);
            } else {
                return call(localTokenInfoEndpointUri, accessToken);
            }
        }

        private Map<String, Object> call(final URI tokenInfoEndpointUri, final String accessToken) {
            LOG.debug("Getting token-info from: {}", tokenInfoEndpointUri.toString());
            final RequestEntity<Void> entity = buildRequestEntity(tokenInfoEndpointUri, accessToken);
            return restTemplate.exchange(entity, TOKENINFO_MAP).getBody();
        }

        public static RequestEntity<Void> buildRequestEntity(final URI tokenInfoEndpointUri, final String accessToken) {
            return RequestEntity.get(tokenInfoEndpointUri).accept(MediaType.APPLICATION_JSON)
                    .header(AUTHORIZATION, BEARER_TYPE + SPACE + accessToken).build();
        }

    }
}
