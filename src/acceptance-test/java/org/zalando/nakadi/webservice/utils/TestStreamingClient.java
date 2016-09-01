package org.zalando.nakadi.webservice.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import org.zalando.nakadi.config.JsonConfig;
import org.zalando.nakadi.webservice.hila.StreamBatch;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Collections;
import java.util.List;

import static java.text.MessageFormat.format;

public class TestStreamingClient implements Runnable {

    private static final ObjectMapper MAPPER = (new JsonConfig()).jacksonObjectMapper();

    private final String baseUrl;
    private final String subscriptionId;
    private final String params;
    private volatile boolean running;

    private final List<StreamBatch> batches;
    private InputStream inputStream;
    private String sessionId;

    public TestStreamingClient(final String baseUrl, final String subscriptionId, final String params) {
        this.baseUrl = baseUrl;
        this.subscriptionId = subscriptionId;
        this.params = params;
        this.batches = Lists.newArrayList();
        this.running = false;
        this.sessionId = "UNKNOWN";
    }

    public static TestStreamingClient create(final String baseUrl, final String subscriptionId, final String params) {
        return new TestStreamingClient(baseUrl, subscriptionId, params);
    }

    @Override
    public void run() {
        try {
            final String url = format("{0}/subscriptions/{1}/events?{2}", baseUrl, subscriptionId, params);
            final HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
            if (conn.getResponseCode() != HttpURLConnection.HTTP_OK) {
                throw new IOException("Response code is " + conn.getResponseCode());
            }
            sessionId = conn.getHeaderField("X-Nakadi-SessionId");
            inputStream = conn.getInputStream();
            final BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
            running = true;

            try {
                while (true) {
                    final String line = reader.readLine();
                    if (line != null) {
                        final StreamBatch streamBatch = MAPPER.readValue(line, StreamBatch.class);
                        batches.add(streamBatch);
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
            e.printStackTrace();
        } finally {
            running = false;
        }
    }

    public TestStreamingClient start() {
        if (!running) {
            batches.clear();
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
                inputStream.close();
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                running = false;
            }
            return true;
        } else {
            return false;
        }
    }

    public List<StreamBatch> getBatches() {
        return Collections.unmodifiableList(batches);
    }

    public boolean isRunning() {
        return running;
    }

    public String getSessionId() {
        return sessionId;
    }
}