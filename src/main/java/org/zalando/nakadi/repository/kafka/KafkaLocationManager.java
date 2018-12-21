package org.zalando.nakadi.repository.kafka;

import org.apache.curator.framework.CuratorFramework;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.zalando.nakadi.repository.zookeeper.ZooKeeperHolder;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class KafkaLocationManager {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaLocationManager.class);
    private static final String BROKERS_IDS_PATH = "/brokers/ids";

    private final ZooKeeperHolder zkFactory;
    private final Properties kafkaProperties;
    private final KafkaSettings kafkaSettings;

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
                } catch (final Exception e) {
                    LOG.info(String.format("Failed to fetch connection string for broker %s", brokerId), e);
                }
            }
        } catch (final Exception e) {
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
        return props;
    }

    @Scheduled(fixedDelay = 30000)
    private void updateBrokers() {
        if (kafkaProperties != null) {
            final List<Broker> brokers = fetchBrokers();
            if (!brokers.isEmpty()) {
                kafkaProperties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
                        buildBootstrapServers(brokers));
            }
        }
    }

    public Properties getKafkaConsumerProperties() {
        final Properties properties = (Properties) kafkaProperties.clone();
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, kafkaSettings.getEnableAutoCommit());
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        return properties;
    }

    public Properties getKafkaProducerProperties() {
        final Properties producerProps = (Properties) kafkaProperties.clone();
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
        producerProps.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, kafkaSettings.getRequestTimeoutMs());
        producerProps.put(ProducerConfig.BATCH_SIZE_CONFIG, kafkaSettings.getBatchSize());
        producerProps.put(ProducerConfig.LINGER_MS_CONFIG, kafkaSettings.getLingerMs());
        producerProps.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        producerProps.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, kafkaSettings.getMaxRequestSize());
        producerProps.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, kafkaSettings.getMaxBlockMsConfig());
        return producerProps;
    }
}
