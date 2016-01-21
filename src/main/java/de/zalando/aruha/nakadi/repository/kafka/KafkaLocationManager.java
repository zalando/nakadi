package de.zalando.aruha.nakadi.repository.kafka;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import javax.annotation.PostConstruct;

import org.apache.curator.framework.CuratorFramework;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;

import de.zalando.aruha.nakadi.repository.zookeeper.ZooKeeperHolder;

public class KafkaLocationManager {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaLocationManager.class);

    private final String _BROKERS_IDS_PATH = "/brokers/ids";

    @Autowired
    private ZooKeeperHolder zkFactory;

    private List<Broker> kafkaBrokerList;

    private Properties kafkaProperties;

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

    private Properties buildKafkaProperties() {
        final Properties props = new Properties();
        props.put("bootstrap.servers", buildBootstrapServers(kafkaBrokerList));
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return props;
    }

    @PostConstruct
    private void init() {
        kafkaBrokerList = fetchBrokers();
        kafkaProperties = buildKafkaProperties();
    }

    @Scheduled(fixedDelay = 30000)
    private void updateBrokers() {
        if (kafkaProperties != null) {
            List<Broker> brokers = fetchBrokers();
            if (!brokers.isEmpty()) {
                kafkaBrokerList = Collections.unmodifiableList(brokers);
                kafkaProperties.setProperty("bootstrap.servers", buildBootstrapServers(brokers));
            }
        }
    }

    public List<Broker> getKafkaBrokers() {
        return kafkaBrokerList;
    }

    public Properties getKafkaProperties() {
        return (Properties) kafkaProperties.clone();
    }
}
