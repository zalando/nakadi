package org.zalando.nakadi.webservice.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zalando.nakadi.config.JsonConfig;
import org.zalando.nakadi.generated.avro.ConsumptionBatch;
import org.zalando.nakadi.util.ThreadUtils;
import org.zalando.nakadi.view.SubscriptionCursor;
import org.zalando.nakadi.webservice.BaseAT;
import org.zalando.nakadi.webservice.hila.StreamBatch;

import javax.annotation.Nullable;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;

import static java.text.MessageFormat.format;

public class TestStreamingClient {

    public static final String SESSION_ID_UNKNOWN = "UNKNOWN";
    private static final Logger LOG = LoggerFactory.getLogger(TestStreamingClient.class);
    private static final ObjectMapper MAPPER = (new JsonConfig()).jacksonObjectMapper();
    private final String baseUrl;
    private final String subscriptionId;
    private final String params;
    private final List<StreamBatch> jsonBatches;
    private final List<ConsumptionBatch> binaryBatches;
    private final Map<String, List<String>> headers;
    private volatile boolean running;
    private HttpURLConnection connection;
    private String sessionId;
    private Optional<String> token;
    private final Optional<String> bodyParams;
    private volatile int responseCode;
    private Consumer<List<StreamBatch>> batchesListener;
    private final CountDownLatch started = new CountDownLatch(1);

    public TestStreamingClient(final String baseUrl, final String subscriptionId, final String params,
                               final Optional<String> token, final Optional<String> bodyParams) {
        this.baseUrl = baseUrl;
        this.subscriptionId = subscriptionId;
        this.params = params;
        this.jsonBatches = Lists.newArrayList();
        this.binaryBatches = Lists.newArrayList();
        this.running = false;
        this.sessionId = SESSION_ID_UNKNOWN;
        this.token = Optional.empty();
        this.headers = new ConcurrentHashMap<>();
        this.token = token;
        this.bodyParams = bodyParams;
    }

    public TestStreamingClient(final String baseUrl, final String subscriptionId, final String params) {
        this(baseUrl, subscriptionId, params, Optional.empty(), Optional.empty());
    }

    public TestStreamingClient(final String baseUrl, final String subscriptionId, final String params,
                               final Optional<String> token) {
        this(baseUrl, subscriptionId, params, token, Optional.empty());
    }

    public static TestStreamingClient create(final String baseUrl, final String subscriptionId, final String params) {
        return new TestStreamingClient(baseUrl, subscriptionId, params);
    }

    public static TestStreamingClient create(final String subscriptionId) {
        return new TestStreamingClient(BaseAT.URL, subscriptionId, "");
    }

    private TestStreamingClient startInternal(final boolean wait,
                                              final Runnable action)
            throws InterruptedException {
        if (!running) {
            running = true;
            jsonBatches.clear();
            binaryBatches.clear();
            headers.clear();
            final Thread thread = new Thread(action);
            thread.start();
            if (wait) {
                started.await();
            }
            return this;
        } else {
            throw new IllegalStateException("Client has not yet finished with previous run");
        }
    }

    public TestStreamingClient start() {
        try {
            return startInternal(false, new JsonConsumer());
        } catch (final InterruptedException ignore) {
            throw new RuntimeException(ignore);
        }
    }

    public TestStreamingClient startBinary() {
        try {
            return startInternal(false, new BinaryConsumer());
        } catch (final InterruptedException ignore) {
            throw new RuntimeException(ignore);
        }
    }

    public TestStreamingClient startWithAutocommit(final Consumer<List<StreamBatch>> batchesListener)
            throws InterruptedException {
        this.batchesListener = batchesListener;
        final TestStreamingClient client = startInternal(true, new JsonConsumer());
        final Thread autocommitThread = new Thread(() -> {
            int oldIdx = 0;
            while (client.isRunning()) {
                while (oldIdx < client.getJsonBatches().size()) {
                    final StreamBatch batch = client.getJsonBatches().get(oldIdx);
                    if (batch.getEvents() != null && !batch.getEvents().isEmpty()) {
                        try {
                            final SubscriptionCursor cursor = batch.getCursor();
                            final int responseCode = NakadiTestUtils.commitCursors(
                                    client.subscriptionId,
                                    Collections.singletonList(batch.getCursor()),
                                    client.getSessionId());
                            LOG.info("Committing " + responseCode + ": " + cursor);
                        } catch (JsonProcessingException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    oldIdx += 1;
                }
                try {
                    ThreadUtils.sleep(100);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        autocommitThread.setDaemon(true);
        autocommitThread.start();
        return client;
    }

    public boolean close() {
        if (running) {
            running = false;
            connection.disconnect();
            return true;
        } else {
            return false;
        }
    }

    public List<StreamBatch> getJsonBatches() {
        synchronized (jsonBatches) {
            return new ArrayList<>(jsonBatches);
        }
    }

    public List<ConsumptionBatch> getBinaryBatches() {
        synchronized (binaryBatches) {
            return new ArrayList<>(binaryBatches);
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

    private abstract class ConsumerThread implements Runnable {

        abstract void addHeaders();

        abstract void readBatches(InputStream inputStream) throws IOException;

        abstract void callListener();

        @Override
        public void run() {
            try {
                final String url = format("{0}/subscriptions/{1}/events?{2}", baseUrl, subscriptionId, params);
                connection = (HttpURLConnection) new URL(url).openConnection();
                addHeaders();
                token.ifPresent(token -> connection.setRequestProperty("Authorization", "Bearer " + token));

                if (bodyParams.isPresent()) {
                    connection.setRequestMethod("POST");
                    connection.setDoOutput(true);
                    connection.setRequestProperty("Content-Type", "application/json");
                    try (DataOutputStream wr = new DataOutputStream(connection.getOutputStream())) {
                        wr.write(bodyParams.get().getBytes(Charsets.UTF_8));
                        wr.flush();
                    }
                }

                responseCode = connection.getResponseCode();
                connection.getHeaderFields().entrySet().stream()
                        .filter(entry -> entry.getKey() != null)
                        .forEach(entry -> headers.put(entry.getKey(), entry.getValue()));
                connection.setReadTimeout(10);
                if (responseCode != HttpURLConnection.HTTP_OK) {
                    throw new IOException("Response code is " + responseCode);
                }
                started.countDown();
                sessionId = connection.getHeaderField("X-Nakadi-StreamId");
                try (InputStream inputStream = connection.getInputStream()) {
                    readBatches(inputStream);
                }
            } catch (IOException e) {
                LOG.error(e.getMessage(), e);
            } finally {
                callListener();
                close();
            }
        }
    }

    private class JsonConsumer extends ConsumerThread {

        @Override
        void addHeaders() {
        }

        @Override
        void readBatches(final InputStream inputStream) throws IOException {
            LOG.info("Started streaming JSON batches");
            final BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, Charsets.UTF_8));
            while (running) {
                try {
                    final String line = reader.readLine();
                    if (line == null) {
                        return;
                    }
                    final StreamBatch streamBatch = MAPPER.readValue(line, StreamBatch.class);
                    synchronized (jsonBatches) {
                        jsonBatches.add(streamBatch);
                    }
                } catch (final SocketTimeoutException ste) {
                    LOG.info("No data in 10 ms, retrying read data");
                }
            }
        }

        @Override
        void callListener() {
            if (null != batchesListener) {
                batchesListener.accept(jsonBatches);
            }
        }
    }

    private class BinaryConsumer extends ConsumerThread {

        @Override
        void addHeaders() {
            connection.setRequestProperty("Accept", "application/avro-binary");
        }

        @Override
        void readBatches(final InputStream inputStream) throws IOException {
            LOG.info("Started streaming binary batches");
            while (running) {
                final ConsumptionBatch consumptionBatch =
                        ConsumptionBatch.getDecoder().decode(inputStream);
                synchronized (binaryBatches) {
                    binaryBatches.add(consumptionBatch);
                }
            }
        }

        @Override
        void callListener() {
        }
    }
}
