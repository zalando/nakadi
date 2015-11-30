package de.zalando.aruha.nakadi.repository.kafka;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;

import de.zalando.aruha.nakadi.NakadiException;
import de.zalando.aruha.nakadi.domain.Topic;
import de.zalando.aruha.nakadi.domain.TopicPartition;
import de.zalando.aruha.nakadi.repository.TopicRepository;
import de.zalando.aruha.nakadi.repository.zookeeper.ZooKeeperHolder;

@Component
public class KafkaRepository implements TopicRepository {

	private static final Logger LOG = LoggerFactory.getLogger(KafkaRepository.class);

	@Autowired
	private Factory factory;
	@Autowired
	private ZooKeeperHolder zkFactory;

	@Override
	public List<Topic> listTopics() throws NakadiException {
		try {
			return zkFactory.get().getChildren("/brokers/topics", false).stream().map(s -> new Topic(s))
					.collect(Collectors.toList());
		} catch (KeeperException | InterruptedException | IOException e) {
			throw new NakadiException("Failed to get partitions", e);
		}
	}

	@Override
	public void postEvent(final String topicId, final String partitionId, final String payload) {
		LOG.info("%s %s %s", topicId, partitionId, payload);

		final ProducerRecord<String, String> record = new ProducerRecord<>(topicId, partitionId, payload);
		factory.createProducer().send(record);
	}

	@Override
	public List<TopicPartition> listPartitions(final String topicId) throws NakadiException {

		try {
			return zkFactory.get().getChildren(String.format("/brokers/topics/%s/partitions", topicId), false).stream()
					.map(p -> new TopicPartition(topicId, p)).collect(Collectors.toList());
		} catch (KeeperException | InterruptedException | IOException e) {
			throw new NakadiException("Failed to get partitions", e);
		}

	}
}

@Component
@PropertySource("${nakadi.config}")
class Factory {

	@Value("${nakadi.kafka.broker}")
	private String kafkaAddress;

	@Value("${nakadi.zookeeper.brokers}")
	private String zookeeperAddress;

	public Factory() {
	}

	public Producer<String, String> createProducer() {
		return new KafkaProducer(getProps());
	}

	public Consumer<String, String> createConsumer() {
		return new KafkaConsumer<>(getProps());
	}

	private Properties getProps() {
		final Properties props = new Properties();
		props.put("metadata.broker.list", zookeeperAddress);
		props.put("bootstrap.servers", kafkaAddress);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		return props;
	}
}