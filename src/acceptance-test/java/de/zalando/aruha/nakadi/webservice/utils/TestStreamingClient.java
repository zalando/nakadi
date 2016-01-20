package de.zalando.aruha.nakadi.webservice.utils;

import com.google.common.collect.Lists;
import de.zalando.aruha.nakadi.service.EventStreamConfig;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.text.MessageFormat;
import java.util.List;

public class TestStreamingClient implements Runnable {

    private final String baseUrl;
    private final String subscriptionId;
    private final EventStreamConfig config;
    private final List<List<String>> data;

    public TestStreamingClient(final String baseUrl, final String subscriptionId, final EventStreamConfig config) {
        this.baseUrl = baseUrl;
        this.subscriptionId = subscriptionId;
        this.config = config;
        data = Lists.newArrayList();
    }

    public static TestStreamingClient of(final String baseUrl, final String subscriptionId,
                                         final EventStreamConfig config) {
        return new TestStreamingClient(baseUrl, subscriptionId, config);
    }

    @Override
    public void run() {
        data.clear();
        try {
            String url = MessageFormat.format("{0}/subscriptions/{1}/events?{2}", baseUrl, subscriptionId,
                    configToUrlParams());
            final InputStream inputStream = new URL(url).openStream();
            final BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));

            int metaBatchIndex = 0;
            while (true) {
                final String line = reader.readLine();
                if (line != null) {
                    if (data.size() <= metaBatchIndex) {
                        data.add(Lists.newArrayList());
                    }
                    if (line.length() == 0) {
                        metaBatchIndex++;
                    } else {
                        data.get(metaBatchIndex).add(line);
                    }
                } else {
                    break;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public String configToUrlParams() {
        final StringBuilder builder = new StringBuilder();
        if (config.getBatchLimit() != null) {
            builder.append("batch_limit=").append(config.getBatchLimit()).append("&");
        }
        if (config.getStreamLimit().isPresent()) {
            builder.append("stream_limit=").append(config.getStreamLimit().get()).append("&");
        }
        if (config.getBatchTimeout().isPresent()) {
            builder.append("batch_flush_timeout=").append(config.getBatchTimeout().get()).append("&");
        }
        if (config.getStreamTimeout().isPresent()) {
            builder.append("stream_timeout=").append(config.getStreamTimeout().get()).append("&");
        }
        if (config.getBatchKeepAliveLimit().isPresent()) {
            builder.append("batch_keep_alive_limit=").append(config.getBatchKeepAliveLimit().get()).append("&");
        }
        String params = builder.toString();
        return params.substring(0, params.length() - 1);
    }

    public TestStreamingClient start() {
        final Thread thread = new Thread(this);
        thread.start();
        return this;
    }

    public List<List<String>> getData() {
        return data;
    }
}
