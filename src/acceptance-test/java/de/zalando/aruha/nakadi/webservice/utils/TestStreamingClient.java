package de.zalando.aruha.nakadi.webservice.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import de.zalando.aruha.nakadi.config.JsonConfig;
import de.zalando.aruha.nakadi.webservice.hila.StreamBatch;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.Collections;
import java.util.List;

import static java.text.MessageFormat.format;

public class TestStreamingClient implements Runnable {

    private static final ObjectMapper mapper = (new JsonConfig()).jacksonObjectMapper();

    private final String baseUrl;
    private final String subscriptionId;
    private final String params;
    private volatile boolean running;

    private final List<StreamBatch> batches;

    public TestStreamingClient(final String baseUrl, final String subscriptionId, final String params) {
        this.baseUrl = baseUrl;
        this.subscriptionId = subscriptionId;
        this.params = params;
        this.batches = Lists.newArrayList();
        this.running = false;
    }

    public static TestStreamingClient create(final String baseUrl, final String subscriptionId, final String params) {
        return new TestStreamingClient(baseUrl, subscriptionId, params);
    }

    @Override
    public void run() {
        try {
            final String url = format("{0}/subscriptions/{1}/events?{2}", baseUrl, subscriptionId, params);
            final InputStream inputStream = new URL(url).openStream();
            final BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));

            try {
                while (true) {
                    final String line = reader.readLine();
                    if (line != null) {
                        final StreamBatch streamBatch = mapper.readValue(line, StreamBatch.class);
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
            running = true;
            batches.clear();
            final Thread thread = new Thread(this);
            thread.start();
            return this;
        } else {
            throw new IllegalStateException("Client has not yet finished with previous run");
        }
    }

    public List<StreamBatch> getBatches() {
        return Collections.unmodifiableList(batches);
    }

    public boolean isRunning() {
        return running;
    }
}