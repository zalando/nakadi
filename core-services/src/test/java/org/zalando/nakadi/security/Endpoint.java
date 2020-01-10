package org.zalando.nakadi.security;

import org.springframework.http.HttpMethod;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;

import javax.annotation.concurrent.Immutable;
import java.util.Optional;

import static org.springframework.http.HttpMethod.DELETE;
import static org.springframework.http.HttpMethod.GET;
import static org.springframework.http.HttpMethod.POST;
import static org.springframework.http.HttpMethod.PUT;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;

@Immutable
public class Endpoint {
    private final HttpMethod method;
    private final String url;
    private final Optional<String> token;

    public Endpoint(final HttpMethod method, final String url) {
        this.method = method;
        this.url = url;
        this.token = Optional.empty();
    }

    public Endpoint(final HttpMethod method, final String url, final String token) {
        this.method = method;
        this.url = url;
        this.token = Optional.of(token);
    }

    public Endpoint withToken(final String token) {
        return null == token ? new Endpoint(this.method, this.url) : new Endpoint(this.method, this.url, token);
    }

    @Override
    public String toString() {
        return method + " " + url;
    }

    public MockHttpServletRequestBuilder toRequestBuilder() {
        final MockHttpServletRequestBuilder result;
        if (method == GET) {
            result = get(url);
        } else if (method == POST) {
            result = post(url);
        } else if (method == PUT) {
            result = put(url);
        } else if (method == DELETE) {
            result = delete(url);
        } else {
            throw new UnsupportedOperationException();
        }
        token.ifPresent(token -> result.header("Authorization", "Bearer " + token));
        return result;
    }
}
