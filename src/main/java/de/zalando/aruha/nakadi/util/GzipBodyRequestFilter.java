package de.zalando.aruha.nakadi.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import java.util.Optional;
import java.util.zip.GZIPInputStream;

import static javax.ws.rs.HttpMethod.POST;
import static javax.ws.rs.core.HttpHeaders.CONTENT_ENCODING;

public class GzipBodyRequestFilter implements Filter {

    private static final Logger LOG = LoggerFactory.getLogger(GzipBodyRequestFilter.class);

    @Override
    public final void doFilter(final ServletRequest servletRequest, final ServletResponse servletResponse,
                               final FilterChain chain) throws IOException, ServletException {

        HttpServletRequest request = (HttpServletRequest) servletRequest;
        final HttpServletResponse response = (HttpServletResponse) servletResponse;

        boolean isGzipped = Optional
                .ofNullable(request.getHeader(CONTENT_ENCODING))
                .map(encoding -> encoding.contains("gzip"))
                .orElse(false);

        if (isGzipped && !POST.equals(request.getMethod())) {
            throw new IllegalStateException(request.getMethod() + " method doesn't support gzip content encoding");
        }
        else if (isGzipped) {
            request = new GzipServletRequestWrapper(request);
        }
        chain.doFilter(request, response);
    }

    @Override
    public void destroy() {
    }

    @Override
    public void init(final FilterConfig filterConfig) throws ServletException {
    }


    private class GzipServletRequestWrapper extends HttpServletRequestWrapper{

        public GzipServletRequestWrapper(final HttpServletRequest request) {
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

        public GzipServletInputStream(InputStream inputStream) throws IOException {
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
            } catch (IOException e) {
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
