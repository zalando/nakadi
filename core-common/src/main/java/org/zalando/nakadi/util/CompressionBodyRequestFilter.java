package org.zalando.nakadi.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.luben.zstd.ZstdInputStream;
import org.springframework.http.HttpMethod;
import org.zalando.problem.Problem;
import org.zalando.problem.Status;

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
import java.util.zip.ZipException;

import static org.springframework.http.HttpHeaders.CONTENT_ENCODING;
import static org.zalando.problem.Status.NOT_ACCEPTABLE;

public class CompressionBodyRequestFilter implements Filter {

    private final ObjectMapper objectMapper;

    public CompressionBodyRequestFilter(final ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public final void doFilter(final ServletRequest servletRequest,
                               final ServletResponse servletResponse,
                               final FilterChain chain)
            throws IOException, ServletException {

        HttpServletRequest request = (HttpServletRequest) servletRequest;
        final HttpServletResponse response = (HttpServletResponse) servletResponse;

        final Optional<String> contentEncodingOpt = Optional.ofNullable(
                request.getHeader(CONTENT_ENCODING));
        if (contentEncodingOpt.isPresent() && !HttpMethod.POST.matches(request.getMethod())) {
            respondWithError(response, Problem.valueOf(NOT_ACCEPTABLE,
                    request.getMethod() + " method doesn't support gzip content encoding"));
            return;
        } else if (contentEncodingOpt.isPresent()) {
            final String contentEncoding = contentEncodingOpt.get();
            if (contentEncoding.contains("gzip")) {
                try {
                    request = new FilterServletRequestWrapper(request, new GZIPInputStream(request.getInputStream()));
                } catch (final ZipException ze) {
                    respondWithError(response, Problem.valueOf(Status.BAD_REQUEST, ze.getMessage()));
                    return;
                }
            } else if (contentEncoding.contains("zstd")) {
                request = new FilterServletRequestWrapper(request, new ZstdInputStream(request.getInputStream()));
            }
        }

        chain.doFilter(request, servletResponse);
    }

    private void respondWithError(final HttpServletResponse response,
                                  final Problem problem)
            throws IOException {
        response.setStatus(problem.getStatus().getStatusCode());
        final PrintWriter writer = response.getWriter();
        writer.write(objectMapper.writeValueAsString(problem));
        writer.close();
    }

    @Override
    public void init(final FilterConfig filterConfig) {
        // filter is stateless, nothing to init
    }

    @Override
    public void destroy() {
        // filter is stateless, nothing to destroy
    }

    static class FilterServletRequestWrapper extends HttpServletRequestWrapper {

        private final ServletInputStreamWrapper inputStreamWrapper;
        private BufferedReader reader;

        FilterServletRequestWrapper(
                final HttpServletRequest request,
                final InputStream decompressedStream) throws IOException {
            super(request);
            this.inputStreamWrapper = new ServletInputStreamWrapper(request.getInputStream(), decompressedStream);
            this.reader = null;
        }

        @Override
        public ServletInputStream getInputStream() throws IOException {
            return inputStreamWrapper;
        }

        @Override
        public BufferedReader getReader() throws IOException {
            if (null == reader) {
                reader = new BufferedReader(new InputStreamReader(inputStreamWrapper));
            }
            return reader;
        }
    }

    private static class ServletInputStreamWrapper extends ServletInputStream {

        private final ServletInputStream originalServletInputStream;
        private final InputStream convertedInputStream;

        private ServletInputStreamWrapper(
                final ServletInputStream originalServletInputStream,
                final InputStream convertedInputStream) {
            this.originalServletInputStream = originalServletInputStream;
            this.convertedInputStream = convertedInputStream;
        }

        @Override
        public int read() throws IOException {
            return convertedInputStream.read();
        }

        @Override
        public int read(final byte[] b) throws IOException {
            return convertedInputStream.read(b);
        }

        @Override
        public int read(final byte[] b, final int off, final int len) throws IOException {
            return convertedInputStream.read(b, off, len);
        }

        @Override
        public void close() throws IOException {
            convertedInputStream.close();
        }

        @Override
        public boolean isFinished() {
            return originalServletInputStream.isFinished();
        }

        @Override
        public boolean isReady() {
            return originalServletInputStream.isReady();
        }

        @Override
        public void setReadListener(final ReadListener listener) {
            originalServletInputStream.setReadListener(listener);
        }
    }

}
