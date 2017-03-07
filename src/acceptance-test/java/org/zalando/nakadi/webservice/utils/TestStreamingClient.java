package org.zalando.nakadi.webservice.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;
import org.zalando.nakadi.config.JsonConfig;
import org.zalando.nakadi.webservice.hila.StreamBatch;
import static java.text.MessageFormat.format;

public class TestStreamingClient implements Runnable {

    private static final ObjectMapper MAPPER = (new JsonConfig()).jacksonObjectMapper();
    public static final String SESSION_ID_UNKNOWN = "UNKNOWN";

    private final String baseUrl;
    private final String subscriptionId;
    private final String params;
    private volatile boolean running;

    private final List<StreamBatch> batches;
    private HttpURLConnection connection;
    private String sessionId;
    private Optional<String> token;
    private volatile int responseCode;
    private final Map<String, List<String>> headers;

    public TestStreamingClient(final String baseUrl, final String subscriptionId, final String params) {
        this.baseUrl = baseUrl;
        this.subscriptionId = subscriptionId;
        this.params = params;
        this.batches = Lists.newArrayList();
        this.running = false;
        this.sessionId = SESSION_ID_UNKNOWN;
        this.token = Optional.empty();
        this.headers = new ConcurrentHashMap<>();
    }

    public TestStreamingClient(final String baseUrl, final String subscriptionId, final String params,
                               final Optional<String> token) {
        this(baseUrl, subscriptionId, params);
        this.token = token;
    }

    public static TestStreamingClient create(final String baseUrl, final String subscriptionId, final String params) {
        return new TestStreamingClient(baseUrl, subscriptionId, params);
    }

    @Override
    public void run() {
        try {
            final String url = format("{0}/subscriptions/{1}/events?{2}", baseUrl, subscriptionId, params);
            connection = (HttpURLConnection) new URL(url).openConnection();
            token.ifPresent(token -> connection.setRequestProperty("Authorization", "Bearer " + token));
            responseCode = connection.getResponseCode();
            connection.getHeaderFields().entrySet().stream()
                    .filter(entry -> entry.getKey() != null)
                    .forEach(entry ->  headers.put(entry.getKey(), entry.getValue()));
            if (responseCode != HttpURLConnection.HTTP_OK) {
                throw new IOException("Response code is " + responseCode);
            }
            sessionId = connection.getHeaderField("X-Nakadi-StreamId");
            final InputStream inputStream = connection.getInputStream();
            final BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
            running = true;

            try {
                while (true) {
                    final String line = reader.readLine();
                    if (line != null) {
                        final StreamBatch streamBatch = MAPPER.readValue(line, StreamBatch.class);
                        synchronized (batches) {
                            batches.add(streamBatch);
                        }
                    } else {
                        break;
                    }
                }
            } finally {
                try {
                    inputStream.close();
                    reader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        } catch (IOException e) {
//            e.printStackTrace();
        } finally {
            running = false;
        }
    }

    public TestStreamingClient start() {
        if (!running) {
            synchronized (batches) {
                batches.clear();
            }
            headers.clear();
            final Thread thread = new Thread(this);
            thread.start();
            return this;
        } else {
            throw new IllegalStateException("Client has not yet finished with previous run");
        }
    }

    public boolean close() {
        if (running) {
            try {
                connection.disconnect();
            } finally {
                running = false;
            }
            return true;
        } else {
            return false;
        }
    }

    public List<StreamBatch> getBatches() {
        synchronized (batches) {
            return new ArrayList<>(batches);
        }
    }

    public boolean isRunning() {
        return running;
    }

    public String getSessionId() {
        return sessionId;
    }

    public int getResponseCode() {
        return responseCode;
    }

    public String getSubscriptionId() {
        return subscriptionId;
    }

    @Nullable
    public String getHeaderValue(final String name) {
        final List<String> values = headers.get(name);
        if (values == null) {
            return null;
        }
        return values.get(0);
    }
}