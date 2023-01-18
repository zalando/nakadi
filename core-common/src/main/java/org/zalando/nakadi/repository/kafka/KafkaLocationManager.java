package org.zalando.nakadi.repository.kafka;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.GetChildrenBuilder;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.zookeeper.Watcher;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zalando.nakadi.repository.zookeeper.ZooKeeperHolder;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class KafkaLocationManager {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaLocationManager.class);
    private static final String BROKERS_IDS_PATH = "/brokers/ids";

    private final ZooKeeperHolder zkFactory;
    private final Properties kafkaProperties;
    private final KafkaSettings kafkaSettings;
    private final ScheduledExecutorService scheduledExecutor;
    private final Set<Runnable> ipAddressChangeListeners;

    public KafkaLocationManager(final ZooKeeperHolder zkFactory, final KafkaSettings kafkaSettings) {
        this.zkFactory = zkFactory;
        this.kafkaProperties = new Properties();
        this.kafkaSettings = kafkaSettings;
        this.ipAddressChangeListeners = ConcurrentHashMap.newKeySet();
        this.updateBootstrapServers(true);
        this.scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
        this.scheduledExecutor.scheduleAtFixedRate(() -> updateBootstrapServersSafe(false), 1, 1, TimeUnit.MINUTES);
    }

    private void updateBootstrapServersSafe(final boolean createWatcher) {
        try {
            updateBootstrapServers(createWatcher);
        } catch (final RuntimeException re) {
            LOG.error("Failed to execute updateBootstrapServers", re);
        }
    }

    public Properties getProperties() {
        return (Properties) kafkaProperties.clone();
    }

    private void updateBootstrapServers(final boolean createWatcher) {
        final List<Broker> brokers = new ArrayList<>();
        try {
            final CuratorFramework curator = zkFactory.get();
            final GetChildrenBuilder childrenBuilder = curator.getChildren();
            if (createWatcher) {
                LOG.info("Creating watcher on brokers change");
                childrenBuilder.usingWatcher((Watcher) event -> {
                    if (event.getType() != Watcher.Event.EventType.NodeChildrenChanged) {
                        return;
                    }
                    this.scheduledExecutor.schedule(() -> updateBootstrapServersSafe(true), 0, TimeUnit.MILLISECONDS);
                });
            }
            for (final String brokerId : childrenBuilder.forPath(BROKERS_IDS_PATH)) {
                final byte[] brokerData = curator.getData().forPath(BROKERS_IDS_PATH + "/" + brokerId);
                brokers.add(Broker.fromByteJson(brokerData));
            }
        } catch (final Exception e) {
            LOG.error("Failed to fetch list of brokers from ZooKeeper", e);
            return;
        }

        if (brokers.isEmpty()) {
            return;
        }
        final String bootstrapServers = brokers.stream()
                .sorted()
                .map(Broker::toString)
                .collect(Collectors.joining(","));
        final String currentBootstrapServers =
                (String) kafkaProperties.get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG);

        if (bootstrapServers.equals(currentBootstrapServers)) {
            return;
        }

        kafkaProperties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        this.ipAddressChangeListeners.forEach(listener -> {
            try {
                listener.run();
            } catch (final RuntimeException re) {
                LOG.error("Failed to process listener {}", re.getMessage(), re);
            }
        });
        LOG.info("Kafka client bootstrap servers changed: {}", bootstrapServers);
    }

    public Properties getKafkaConsumerProperties() {
        final Properties properties = (Properties) kafkaProperties.clone();
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, kafkaSettings.getEnableAutoCommit());
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        properties.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, kafkaSettings.getMaxBlockMs());
        properties.put(ConsumerConfig.CLIENT_RACK_CONFIG, kafkaSettings.getClientRack());
        return properties;
    }

    public Properties getKafkaProducerProperties(final String clientId) {
        final Properties producerProps = (Properties) kafkaProperties.clone();

        producerProps.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);

        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArraySerializer");
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArraySerializer");
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
        producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);

        producerProps.put(ProducerConfig.RETRIES_CONFIG, kafkaSettings.getRetries());
        producerProps.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, kafkaSettings.getRequestTimeoutMs());
        producerProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG, kafkaSettings.getBufferMemory());
        producerProps.put(ProducerConfig.BATCH_SIZE_CONFIG, kafkaSettings.getBatchSize());
        producerProps.put(ProducerConfig.LINGER_MS_CONFIG, kafkaSettings.getLingerMs());
        producerProps.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, kafkaSettings.getMaxInFlightRequests());
        producerProps.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, kafkaSettings.getCompressionType());
        producerProps.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, kafkaSettings.getMaxRequestSize());
        producerProps.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, kafkaSettings.getDeliveryTimeoutMs());
        producerProps.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, kafkaSettings.getMaxBlockMs());
        producerProps.put(ProducerConfig.METADATA_MAX_AGE_CONFIG, kafkaSettings.getMetadataMaxAgeMs());
        return producerProps;
    }

    public void addIpAddressChangeListener(final Runnable listener) {
        this.ipAddressChangeListeners.add(listener);
    }

    public void removeIpAddressChangeListener(final Runnable brokerIpAddressChangeListener) {
        this.ipAddressChangeListeners.remove(brokerIpAddressChangeListener);
    }

    private static class Broker implements Comparable<Broker> {
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

        @Override
        public int compareTo(final Broker broker) {
            return host.compareTo(broker.host);
        }
    }
}
