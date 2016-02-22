package de.zalando.aruha.nakadi.security;

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
    private final Optional<String> validToken;

    public Endpoint(final HttpMethod method, final String url) {
        this.method = method;
        this.url = url;
        this.validToken = Optional.empty();
    }

    public Endpoint(final HttpMethod method, final String url, final String validToken) {
        this.method = method;
        this.url = url;
        this.validToken = Optional.of(validToken);
    }

    public HttpMethod getMethod() {
        return method;
    }

    public String getUrl() {
        return url;
    }

    public Optional<String> getValidToken() {
        return validToken;
    }

    @Override
    public String toString() {
        return method + " " + url;
    }

    public MockHttpServletRequestBuilder toRequestBuilder() {
        if (method == GET) {
            return get(url);
        }
        else if (method == POST) {
            return post(url);
        }
        else if (method == PUT) {
            return put(url);
        }
        else if (method == DELETE) {
            return delete(url);
        }
        else {
            throw new UnsupportedOperationException();
        }
    }
}
