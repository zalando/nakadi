package org.zalando.nakadi.plugin.auth;

import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.zalando.nakadi.plugin.api.SystemProperties;
import org.zalando.nakadi.plugin.api.exceptions.PluginException;

import java.net.URISyntaxException;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

public class ServiceFactory {

    private final SystemProperties properties;
    private HttpClient httpClient;
    private TokenProvider tokenProvider;
    private ValueRegistry merchantRegistry;
    private ValueRegistry applicationRegistry;
    private ValueRegistry userRegistry;

    public ServiceFactory(final SystemProperties properties) {
        this.properties = properties;
    }

    public String getProperty(final String key) {
        final String property = properties.getProperty(key);
        if (property == null) {
            throw new PluginException("Property " + key + " isn't specified");
        }
        return property;
    }

    public HttpClient getOrCreateHttpClient() {
        if (null == httpClient) {
            httpClient = HttpClientBuilder.create()
                    .setUserAgent("nakadi")
                    .evictIdleConnections(1L, TimeUnit.MINUTES)
                    .evictExpiredConnections()
                    .build();
        }
        return httpClient;
    }

    public TokenProvider getOrCreateTokenProvider() throws URISyntaxException {
        if (null == tokenProvider) {
            final String accessTokenUri = getProperty("nakadi.plugins.authz.auth-token-uri");
            final String tokenId = getProperty("nakadi.plugins.authz.token-id");

            tokenProvider = new TokenProvider(accessTokenUri, tokenId);
        }
        return tokenProvider;
    }

    public ValueRegistry getOrCreateMerchantRegistry()
            throws URISyntaxException {
        if (null == merchantRegistry) {
            merchantRegistry = createMerchantRegistryInternal(
                    getProperty("nakadi.plugins.authz.merchant-profile-uri") + "/merchants",
                    getOrCreateHttpClient(),
                    getOrCreateTokenProvider()
            );
        }
        return merchantRegistry;
    }

    static ValueRegistry createMerchantRegistryInternal(
            final String url, final HttpClient httpClient, final TokenProvider tokenProvider) {
        return new ValueRegistry.HttpValidatedValueRegistry(
                tokenProvider,
                httpClient,
                url,
                new int[]{HttpStatus.SC_OK},
                new int[]{HttpStatus.SC_NOT_FOUND, HttpStatus.SC_BAD_REQUEST});
    }

    public ValueRegistry getOrCreateApplicationRegistry()
            throws URISyntaxException {
        if (null == applicationRegistry) {
            applicationRegistry = createApplicationRegistryInternal(
                    getProperty("nakadi.plugins.authz.services-endpoint"),
                    getOrCreateHttpClient(),
                    getOrCreateTokenProvider()
            );
        }
        return applicationRegistry;
    }

    static ValueRegistry createApplicationRegistryInternal(
            final String servicesEndpoint, final HttpClient httpClient, final TokenProvider tokenProvider) {
        return new ValueRegistry.FilteringValueRegistry(
                new ValueRegistry.PatternFilter(Pattern.compile("^[a-z][a-z0-9-]*[a-z0-9]$")),
                new ValueRegistry.HttpValidatedValueRegistry(
                        tokenProvider,
                        httpClient,
                        servicesEndpoint,
                        new int[]{HttpStatus.SC_OK},
                        new int[]{HttpStatus.SC_NOT_FOUND}));
    }

    public ValueRegistry getOrCreateUserRegistry() throws URISyntaxException {
        if (null == userRegistry) {
            final boolean validatePrincipalExists = Boolean.parseBoolean(
                    getProperty("nakadi.plugins.authz.validate-principal-exists"));
            if (!validatePrincipalExists) {
                userRegistry = value -> true;
            } else {
                userRegistry = createUserRegistryInternal(
                        getProperty("nakadi.plugins.authz.users-endpoint"),
                        getOrCreateHttpClient(),
                        getOrCreateTokenProvider()
                );
            }
        }
        return userRegistry;
    }

    static ValueRegistry createUserRegistryInternal(
            final String usersEndpoint, final HttpClient httpClient, final TokenProvider tokenProvider) {
        return new ValueRegistry.FilteringValueRegistry(
                new ValueRegistry.PatternFilter(Pattern.compile("^[a-z][a-z0-9-]*[a-z0-9]$")),
                new ValueRegistry.HttpValidatedValueRegistry(
                        tokenProvider,
                        httpClient,
                        usersEndpoint,
                        new int[]{HttpStatus.SC_OK},
                        new int[]{HttpStatus.SC_NOT_FOUND})
        );
    }
}
