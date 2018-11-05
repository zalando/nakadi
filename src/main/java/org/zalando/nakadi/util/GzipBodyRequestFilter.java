package org.zalando.nakadi.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zalando.problem.Problem;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ReadListener;
import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.Optional;
import java.util.zip.GZIPInputStream;

import static org.springframework.http.HttpHeaders.CONTENT_ENCODING;
import static org.springframework.http.HttpMethod.POST;
import static org.zalando.problem.Status.NOT_ACCEPTABLE;

public class GzipBodyRequestFilter implements Filter {

    private static final Logger LOG = LoggerFactory.getLogger(GzipBodyRequestFilter.class);

    private final ObjectMapper objectMapper;

    public GzipBodyRequestFilter(final ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public final void doFilter(final ServletRequest servletRequest, final ServletResponse servletResponse,
                               final FilterChain chain) throws IOException, ServletException {

        HttpServletRequest request = (HttpServletRequest) servletRequest;

        final boolean isGzipped = Optional
                .ofNullable(request.getHeader(CONTENT_ENCODING))
                .map(encoding -> encoding.contains("gzip"))
                .orElse(false);

        if (isGzipped && !POST.matches(request.getMethod())) {
            reportNotAcceptableError((HttpServletResponse) servletResponse, request);
            return;
        }
        else if (isGzipped) {
            request = new GzipServletRequestWrapper(request);
        }
        chain.doFilter(request, servletResponse);
    }

    private void reportNotAcceptableError(final HttpServletResponse response, final HttpServletRequest request)
            throws IOException {

        response.setStatus(NOT_ACCEPTABLE.getStatusCode());
        final PrintWriter writer = response.getWriter();
        final Problem problem = Problem.valueOf(NOT_ACCEPTABLE,
                request.getMethod() + " method doesn't support gzip content encoding");
        writer.write(objectMapper.writeValueAsString(problem));
        writer.close();
    }

    @Override
    public void init(final FilterConfig filterConfig) throws ServletException {
        // filter is stateless, nothing to init
    }

    @Override
    public void destroy() {
        // filter is stateless, nothing to destroy
    }


    private class GzipServletRequestWrapper extends HttpServletRequestWrapper{

        GzipServletRequestWrapper(final HttpServletRequest request) {
            super(request);
        }

        @Override
        public ServletInputStream getInputStream() throws IOException {
            return new GzipServletInputStream(super.getInputStream());
        }

        @Override
        public BufferedReader getReader() throws IOException {
            return new BufferedReader(new InputStreamReader(this.getInputStream()));
        }
    }


    private class GzipServletInputStream extends ServletInputStream {

        private final InputStream inputStream;

        GzipServletInputStream(final InputStream inputStream) throws IOException {
            super();
            this.inputStream = new GZIPInputStream(inputStream);
        }

        @Override
        public int read() throws IOException {
            return inputStream.read();
        }

        @Override
        public void close() throws IOException {
            inputStream.close();
        }

        @Override
        public boolean isFinished() {
            try {
                return inputStream.available() == 0;
            } catch (final IOException e) {
                LOG.error("Error occurred when reading request input stream", e);
                return false;
            }
        }

        @Override
        public boolean isReady() {
            return true;
        }

        @Override
        public void setReadListener(final ReadListener listener) {
            throw new UnsupportedOperationException("Not supported");
        }
    }

}
