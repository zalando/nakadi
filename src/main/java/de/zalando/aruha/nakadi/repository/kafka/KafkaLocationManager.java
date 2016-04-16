package de.zalando.aruha.nakadi.repository.kafka;

import de.zalando.aruha.nakadi.repository.zookeeper.ZooKeeperHolder;
import org.apache.curator.framework.CuratorFramework;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

class KafkaLocationManager {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaLocationManager.class);

    private static final String _BROKERS_IDS_PATH = "/brokers/ids";

    private final ZooKeeperHolder zkFactory;

    private volatile Properties kafkaProperties;
    private final List<KafkaPropertiesListener> kafkaPropertiesListeners = new LinkedList<>();

    public KafkaLocationManager(final ZooKeeperHolder zkFactory) {
        this.zkFactory = zkFactory;
        init();
    }

    public void registerPropertiesListener(final KafkaPropertiesListener listener) {
        kafkaPropertiesListeners.add(listener);
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
        final List<Broker> brokers = new ArrayList<Broker>();
        try {
            final CuratorFramework curator = zkFactory.get();
            for (final String brokerId : curator.getChildren().forPath(_BROKERS_IDS_PATH)) {
                try {
                    final byte[] brokerData = curator.getData().forPath(_BROKERS_IDS_PATH + "/" + brokerId);
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

    private static String buildBootstrapServers(List<Broker> brokers) {
        final StringBuilder builder = new StringBuilder();
        brokers.stream().forEach(broker -> builder.append(broker).append(","));
        return builder.deleteCharAt(builder.length() - 1).toString();
    }

    private Properties buildKafkaProperties(final List<Broker> brokers) {
        final Properties props = new Properties();
        props.put("bootstrap.servers", buildBootstrapServers(brokers));
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("acks", "all");
        return props;
    }

    private void init() {
        kafkaProperties = buildKafkaProperties(fetchBrokers());
    }

    @Scheduled(fixedDelay = 30000)
    private void updateBrokers() {
        if (kafkaProperties != null) {
            final Properties oldProperties = getKafkaProperties();

            final List<Broker> brokers = fetchBrokers();
            if (!brokers.isEmpty()) {
                this.kafkaProperties.setProperty("bootstrap.servers", buildBootstrapServers(brokers));
            }

            if (!oldProperties.getProperty("bootstrap.servers").equals(kafkaProperties.getProperty("bootstrap.servers"))) {
                notifyPropertiesListeners();
            }
        }
    }

    private void notifyPropertiesListeners() {
        kafkaPropertiesListeners.stream().forEach(listener -> listener.updateProperties(getKafkaProperties()));
    }

    public Properties getKafkaProperties() {
        return (Properties) kafkaProperties.clone();
    }
}
