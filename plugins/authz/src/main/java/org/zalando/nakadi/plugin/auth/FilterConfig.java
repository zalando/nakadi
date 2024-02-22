package org.zalando.nakadi.plugin.auth;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import okhttp3.OkHttpClient;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.zalando.nakadi.plugin.auth.subject.PrincipalFactory;
import org.zalando.problem.ProblemModule;

import java.net.URISyntaxException;
import java.security.PublicKey;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static org.zalando.nakadi.plugin.auth.MerchantGatewayService.loadPublicKey;

@Configuration
public class FilterConfig {

    @Value("${nakadi.plugins.authz.merchant.url}")
    private String merchantUrl;
    @Value("${nakadi.plugins.authz.merchant.uids}")
    private String merchantUids;
    @Value("${nakadi.plugins.authz.merchant.scope}")
    private String merchantScope;
    @Value("${nakadi.plugins.authz.merchant.cache.size:100000}")
    private Integer merchantCacheSize;
    @Value("${nakadi.plugins.authz.merchant.cache.expire:5}")
    private Long merchantCacheExpirationMins;

    @Value("${nakadi.plugins.authz.usersRealm}")
    private String usersRealm;
    @Value("${nakadi.plugins.authz.usersType}")
    private String usersType;
    @Value("${nakadi.plugins.authz.servicesRealm}")
    private String servicesRealm;
    @Value("${nakadi.plugins.authz.servicesType}")
    private String servicesType;

    @Value("${nakadi.plugins.authz.principal.cache.size:10000}")
    private Integer principalCacheSize;
    @Value("${nakadi.plugins.authz.principal.cache.expire:5}")
    private Long principalCacheExpirationMins;

    //
    // The following filters are relying on us re-defining the order of Spring's security filter in
    // the main Nakadi code to be 0 instead of the default (lowest precedence).
    //
    // See: https://github.com/zalando/nakadi/blob/master/app/src/main/java/org/zalando/nakadi/config/WebConfig.java
    //
    @Bean
    public FilterRegistrationBean xConsumerFilterBean(final ProblemWriter problemWriter) {
        final FilterRegistrationBean registrationBean = new FilterRegistrationBean();
        final XConsumerHeaderFilter xConsumerHeaderFilter = new XConsumerHeaderFilter(
                new MerchantGatewayService(pubkeyCache()),
                Arrays.asList(merchantUids.split("\\s*,\\s*")), merchantScope, problemWriter);
        registrationBean.setFilter(xConsumerHeaderFilter);
        registrationBean.setOrder(1);
        return registrationBean;
    }

    @Bean
    public FilterRegistrationBean principalCreatorFilterBean(
            final PrincipalFactory principalFactory, final ProblemWriter problemWriter) {
        final FilterRegistrationBean registrationBean = new FilterRegistrationBean();
        final PrincipalCreatorFilter principalCreatorFilter = new PrincipalCreatorFilter(
                principalFactory, problemWriter, principalCacheSize, principalCacheExpirationMins);
        registrationBean.setFilter(principalCreatorFilter);
        registrationBean.setOrder(2);
        return registrationBean;
    }

    @Bean
    public ProblemWriter problemWriter(final ObjectMapper objectMapper) {
        // It is expected that main nakadi ObjectMapper will be used (and it already has Problem module),
        // but as we are living in a higly unstable environment - will try to register this module second time
        objectMapper.registerModule(new ProblemModule());
        return new ProblemWriter(objectMapper);
    }

    @Bean
    public OPAClient opaClient(
            @Value("${nakadi.plugins.authz.opa.endpoint}") final String endpoint,
            @Value("${nakadi.plugins.authz.opa.policypath}") final String policyPath,
            @Qualifier("tokenProviderForAuthzPlugin") final TokenProvider tokenProvider,
            @Value("${nakadi.plugins.authz.opa.timeoutms.connect:60}") final long timeoutConnect,
            @Value("${nakadi.plugins.authz.opa.timeoutms.write:60}") final long timeoutWrite,
            @Value("${nakadi.plugins.authz.opa.timeoutms.read:80}") final long timeoutRead,
            @Value("${nakadi.plugins.authz.opa.retry.timeout:100}") final long retryTimeout,
            @Value("${nakadi.plugins.authz.opa.retry.times:1}") final int retryTimes,
            @Value("${nakadi.plugins.authz.opa.degradation:THROW}") final OpaDegradationPolicy opaDegradationPolicy) {

        final OkHttpClient client = new OkHttpClient.Builder()
                .connectTimeout(timeoutConnect, TimeUnit.MILLISECONDS)
                .writeTimeout(timeoutWrite, TimeUnit.MILLISECONDS)
                .readTimeout(timeoutRead, TimeUnit.MILLISECONDS)
                .retryOnConnectionFailure(false)
                .build();
        return new OPAClient(
                client, tokenProvider,
                endpoint, policyPath,
                retryTimeout, retryTimes,
                opaDegradationPolicy);
    }

    @Bean
    public TokenProvider tokenProviderForAuthzPlugin(
            @Value("${nakadi.plugins.authz.authTokenUri}") final String accessTokenUri,
            @Value("${nakadi.plugins.authz.tokenId}") final String tokenId) throws URISyntaxException {
        return new TokenProvider(accessTokenUri, tokenId);
    }

    @Bean
    public ZalandoTeamService teamService(
            @Value("${nakadi.plugins.authz.teams-endpoint}") final String teamsEndpoint,
            @Qualifier("tokenProviderForAuthzPlugin") final TokenProvider tokenProvider,
            final ObjectMapper objectMapper) {

        final HttpClient httpClient = HttpClientBuilder.create()
                .setUserAgent("nakadi")
                .evictIdleConnections(1L, TimeUnit.MINUTES)
                .evictExpiredConnections()
                .build();

        return new ZalandoTeamService(teamsEndpoint, httpClient, tokenProvider, objectMapper);
    }

    @Bean
    public PrincipalFactory subjectFactory(final OPAClient opaClient, final ZalandoTeamService teamService) {
        return new PrincipalFactory(usersRealm, usersType, servicesRealm, servicesType, opaClient, teamService);
    }

    @Bean
    public LoadingCache<String, PublicKey> pubkeyCache() {
        final CacheLoader<String, PublicKey> loader = new CacheLoader<String, PublicKey>() {
            public PublicKey load(final String keyId) throws Exception {
                return loadPublicKey(merchantUrl + "/.well-known/public-keys/consumer");
            }
        };

        return CacheBuilder.newBuilder()
                .maximumSize(merchantCacheSize)
                .expireAfterWrite(merchantCacheExpirationMins, TimeUnit.MINUTES)
                .build(loader);
    }

}
