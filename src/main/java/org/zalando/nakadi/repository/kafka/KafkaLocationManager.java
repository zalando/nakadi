package org.zalando.nakadi.repository.kafka;

import org.apache.curator.framework.CuratorFramework;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.repository.zookeeper.ZooKeeperHolder;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

@Component
@Profile("!test")
public class KafkaLocationManager {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaLocationManager.class);
    private static final String BROKERS_IDS_PATH = "/brokers/ids";

    private final ZooKeeperHolder zkFactory;
    private final Properties kafkaProperties;
    private final KafkaSettings kafkaSettings;

    @Autowired
    public KafkaLocationManager(final ZooKeeperHolder zkFactory, final KafkaSettings kafkaSettings) {
        this.zkFactory = zkFactory;
        this.kafkaProperties = buildKafkaProperties(fetchBrokers());
        this.kafkaSettings = kafkaSettings;
    }

    static class Broker {
        final String host;
        final Integer port;

        private Broker(final String host, final Integer port) {
            this.host = host;
            this.port = port;
        }

        static Broker fromByteJson(final byte[] data) throws JSONException, UnsupportedEncodingException {
            final JSONObject json = new JSONObject(new String(data, "UTF-8"));
            final String host = json.getString("host");
            final Integer port = json.getInt("port");
            return new Broker(host, port);
        }

        public String toString() {
            return this.host + ":" + this.port;
        }
    }

    private List<Broker> fetchBrokers() {
        final List<Broker> brokers = new ArrayList<>();
        try {
            final CuratorFramework curator = zkFactory.get();
            for (final String brokerId : curator.getChildren().forPath(BROKERS_IDS_PATH)) {
                try {
                    final byte[] brokerData = curator.getData().forPath(BROKERS_IDS_PATH + "/" + brokerId);
                    brokers.add(Broker.fromByteJson(brokerData));
                } catch (Exception e) {
                    LOG.info(String.format("Failed to fetch connection string for broker %s", brokerId), e);
                }
            }
        } catch (Exception e) {
            LOG.error("Failed to fetch list of brokers from ZooKeeper", e);
        }

        return brokers;
    }

    private static String buildBootstrapServers(final List<Broker> brokers) {
        final StringBuilder builder = new StringBuilder();
        brokers.stream().forEach(broker -> builder.append(broker).append(","));
        return builder.deleteCharAt(builder.length() - 1).toString();
    }

    private Properties buildKafkaProperties(final List<Broker> brokers) {
        final Properties props = new Properties();
        props.put("bootstrap.servers", buildBootstrapServers(brokers));
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return props;
    }

    @Scheduled(fixedDelay = 30000)
    private void updateBrokers() {
        if (kafkaProperties != null) {
            final List<Broker> brokers = fetchBrokers();
            if (!brokers.isEmpty()) {
                kafkaProperties.setProperty("bootstrap.servers", buildBootstrapServers(brokers));
            }
        }
    }

    public Properties getStreamProperties(final int applicationId) {
        final Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", kafkaProperties.getProperty("bootstrap.servers"));
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "nakadi-streams-" + applicationId);
        properties.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG,
                zkFactory.get().getZookeeperClient().getCurrentConnectionString());
        properties.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName()); // FIXME
        properties.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName()); // FIXME
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);
        return properties;
    }

    public Properties getKafkaConsumerProperties() {
        return (Properties) kafkaProperties.clone();
    }

    public Properties getKafkaProducerProperties() {
        final Properties producerProps = getKafkaConsumerProperties();
        producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("acks", "all");
        producerProps.put("request.timeout.ms", kafkaSettings.getRequestTimeoutMs());
        producerProps.put("batch.size", kafkaSettings.getBatchSize());
        producerProps.put("linger.ms", kafkaSettings.getLingerMs());
        return producerProps;
    }
}
