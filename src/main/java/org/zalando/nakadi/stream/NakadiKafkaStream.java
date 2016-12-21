package org.zalando.nakadi.stream;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zalando.nakadi.stream.expression.ExpressionType;
import org.zalando.nakadi.stream.expression.Interpreter;
import org.zalando.nakadi.stream.expression.NakadiMapper;
import org.zalando.nakadi.stream.expression.NakadiPredicate;

import java.io.IOException;
import java.io.OutputStream;

public final class NakadiKafkaStream implements NakadiStream {

    private static final Logger LOG = LoggerFactory.getLogger(NakadiKafkaStream.class);
    private static final byte[] HEARTBEAT = "{\"heartbeat\": \"90\"}\n".getBytes();
    private static final int HEARTBEAT_TIME = 5 * 1000;
    private final Object lock = new Object();

    @Override
    public void stream(final StreamConfig streamConfig, final Interpreter interpreter) {
        final KStreamBuilder kStreamBuilder = new KStreamBuilder();
        final KStream<String, String> stream = kStreamBuilder.stream(streamConfig.getTopics());
        filter(stream, streamConfig, interpreter);
        final KafkaStreams streams = new KafkaStreams(kStreamBuilder, streamConfig.getKafkaStreamProperties());
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        loopWithHeartbeats(streamConfig, streams);
    }

    private void loopWithHeartbeats(final StreamConfig streamConfig, final KafkaStreams streams) {
        long sendTime = System.currentTimeMillis() + HEARTBEAT_TIME;
        while (true) {
            try {
                Thread.sleep(HEARTBEAT_TIME);
            } catch (InterruptedException e) {
                LOG.error(e.getMessage(), e);
            }
            if (sendTime <= System.currentTimeMillis()) {
                sendTime = System.currentTimeMillis() + HEARTBEAT_TIME;
                try {
                    writeToOutput(streamConfig.getOutputStream(), HEARTBEAT);
                } catch (IOException e) {
                    LOG.error(e.getMessage(), e);
                    streams.close();
                    return;
                }
            }
        }
    }

    public void filter(final KStream<String, String> stream,
                       final StreamConfig streamConfig,
                       final Interpreter interpreter) {
        KStream<String, String> tempStream = stream;
        for (final ExpressionType type : interpreter.getPositions()) {
            switch (type) {
                case FILTER:
                    final NakadiPredicate predicate = interpreter.getNextPredicate();
                    tempStream = tempStream.filter((k, v) -> predicate.getValue().test(v));
                    break;
                case MAP:
                    final NakadiMapper mapper = interpreter.getNextMapper();
                    tempStream = tempStream.mapValues(v -> mapper.getValue().apply(v));
                    break;
            }
        }

        if (streamConfig.isToEventType()) {
            LOG.debug("Writing to event type: {} with topic {}",
                    streamConfig.getOutputEventType(), streamConfig.getOutputTopic());
            tempStream
                    .mapValues((v -> new JSONObject().put("value", v).toString() + "\n"))
                    .to(streamConfig.getOutputTopic());
        } else {
            tempStream.map((k, v) -> {
                try {
                    final String data = new JSONObject().put("value", v).toString() + "\n";
                    writeToOutput(streamConfig.getOutputStream(), data.getBytes());
                } catch (IOException e) {
                    LOG.debug(e.getMessage(), e);
                }
                return KeyValue.pair(k, v);
            });
        }
    }

    private void writeToOutput(final OutputStream outputStream, final byte[] data) throws IOException {
        synchronized (lock) {
            outputStream.write(data);
            outputStream.flush();
        }
    }

}
