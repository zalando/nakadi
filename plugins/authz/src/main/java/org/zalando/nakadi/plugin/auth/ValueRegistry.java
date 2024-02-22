package org.zalando.nakadi.plugin.auth;

import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.entity.ContentType;
import org.apache.http.util.EntityUtils;
import org.zalando.nakadi.plugin.api.exceptions.PluginException;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public interface ValueRegistry {
    boolean isValid(String value) throws PluginException;

    class HttpValidatedValueRegistry implements ValueRegistry {

        private final HttpClient httpClient;
        private final String baseUri;
        private final TokenProvider tokenProvider;
        private final Set<Integer> trueCodes;
        private final Set<Integer> falseCodes;

        public HttpValidatedValueRegistry(
                final TokenProvider tokenProvider,
                final HttpClient httpClient,
                final String baseUri,
                final int[] trueCodes,
                final int[] falseCodes) {
            this.tokenProvider = tokenProvider;
            this.baseUri = baseUri;
            this.httpClient = httpClient;
            this.trueCodes = IntStream.of(trueCodes).boxed().collect(Collectors.toSet());
            this.falseCodes = IntStream.of(falseCodes).boxed().collect(Collectors.toSet());
        }

        public boolean isValid(final String value) throws PluginException {
            try {
                if (value.isEmpty() || !Objects.equals(URLEncoder.encode(value, "utf-8"), value)) {
                    return false;
                }
            } catch (UnsupportedEncodingException ex) {
                throw new PluginException("Failed to check encode value " + value, ex);
            }
            final HttpGet request = new HttpGet(baseUri + "/" + value);
            request.addHeader(HttpHeaders.ACCEPT, ContentType.APPLICATION_JSON.getMimeType());
            request.addHeader(HttpHeaders.AUTHORIZATION, "Bearer " + tokenProvider.getToken());
            try {
                final HttpResponse response = httpClient.execute(request);
                final String responseBody = EntityUtils.toString(response.getEntity());
                final int statusCode = response.getStatusLine().getStatusCode();
                if (trueCodes.contains(statusCode)) {
                    return true;
                } else if (falseCodes.contains(statusCode)) {
                    return false;
                } else {
                    throw new PluginException("Incorrect status code " + statusCode + " when validating " +
                            value + " against " + baseUri + ". Response body: " + responseBody);
                }
            } catch (final IOException ex) {
                throw new PluginException(
                        "Failed to check " + value + " validity against endpoint " + baseUri, ex);
            } finally {
                request.releaseConnection();
            }
        }
    }

    class PatternFilter implements ValueRegistry {
        private final Pattern pattern;

        public PatternFilter(final Pattern pattern) {
            this.pattern = pattern;
        }

        @Override
        public boolean isValid(final String value) throws PluginException {
            return pattern.matcher(value).matches();
        }
    }

    class FilteringValueRegistry implements ValueRegistry {
        private final List<ValueRegistry> registries;

        public FilteringValueRegistry(final ValueRegistry first, final ValueRegistry... others) {
            registries = new ArrayList<>(1 + others.length);
            registries.add(first);
            registries.addAll(Arrays.asList(others));
        }

        @Override
        public boolean isValid(final String value) throws PluginException {
            for (final ValueRegistry vr : registries) {
                if (!vr.isValid(value)) {
                    return false;
                }
            }
            return true;
        }
    }
}