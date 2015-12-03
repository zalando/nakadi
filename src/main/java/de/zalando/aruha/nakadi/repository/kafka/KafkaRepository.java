package de.zalando.aruha.nakadi.repository.kafka;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.network.KafkaChannel;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import de.zalando.aruha.nakadi.NakadiException;
import de.zalando.aruha.nakadi.domain.Topic;
import de.zalando.aruha.nakadi.domain.TopicPartition;
import de.zalando.aruha.nakadi.repository.TopicRepository;
import de.zalando.aruha.nakadi.repository.zookeeper.ZooKeeperHolder;
import kafka.api.FetchRequestBuilder;
import kafka.api.OffsetRequest;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.TopicAndPartition;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.log.Log;

import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.stream.Collectors.*;

@Component
public class KafkaRepository implements TopicRepository {

	private static final Logger LOG = LoggerFactory.getLogger(KafkaRepository.class);

	private static final long KAFKA_SEND_TIMEOUT_MS = 10000;

	private static final long KAFKA_READ_TIMEOUT_MS = 0;

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
	public void postEvent(final String topicId, final String partitionId, final String payload) throws NakadiException {
		LOG.info("Posting {} {} {}", topicId, partitionId, payload);

		final ProducerRecord<String, String> record = new ProducerRecord<>(topicId, Integer.parseInt(partitionId),
				partitionId, payload);
		try {
			factory.getProducer().send(record).get(KAFKA_SEND_TIMEOUT_MS, TimeUnit.MILLISECONDS);
		} catch (InterruptedException | ExecutionException | TimeoutException e) {
			throw new NakadiException("Failed to send event", e);
		}
	}

	@Override
	public List<TopicPartition> listPartitions(final String topicId) throws NakadiException {

		final SimpleConsumer sc = factory.getSimpleConsumer();

		final List<TopicAndPartition> partitions = factory.getConsumer().partitionsFor(topicId).stream()
				.map(p -> new TopicAndPartition(p.topic(), p.partition())).collect(Collectors.toList());

		final Map<TopicAndPartition, PartitionOffsetRequestInfo> latestPartitionRequests = partitions.stream()
				.collect(Collectors.toMap(Function.identity(),
						t -> new PartitionOffsetRequestInfo(kafka.api.OffsetRequest.LatestTime(), 1)));
		final Map<TopicAndPartition, PartitionOffsetRequestInfo> earliestPartitionRequests = partitions.stream()
				.collect(Collectors.toMap(Function.identity(),
						t -> new PartitionOffsetRequestInfo(kafka.api.OffsetRequest.EarliestTime(), 1)));

		final OffsetResponse latestPartitionData = fetchPartitionData(sc, latestPartitionRequests);
		final OffsetResponse earliestPartitionData = fetchPartitionData(sc, earliestPartitionRequests);

		return partitions.stream()
				.map(r -> processTopicPartitionMetadata(r, latestPartitionData, earliestPartitionData))
				.collect(Collectors.toList());

	}

	private TopicPartition processTopicPartitionMetadata(final TopicAndPartition r,
			final OffsetResponse latestPartitionData, final OffsetResponse earliestPartitionData) {
		final TopicPartition tp = new TopicPartition(r.topic(), Integer.toString(r.partition()));
		final long latestOffset = latestPartitionData.offsets(r.topic(), r.partition())[0];
		final long earliestOffset = earliestPartitionData.offsets(r.topic(), r.partition())[0];

		tp.setNewestAvailableOffset(Long.toString(latestOffset));
		tp.setOldestAvailableOffset(Long.toString(earliestOffset));

		return tp;
	}

	private OffsetResponse fetchPartitionData(final SimpleConsumer sc,
			final Map<TopicAndPartition, PartitionOffsetRequestInfo> partitionRequests) {
		final kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(partitionRequests,
				OffsetRequest.CurrentVersion(), "offsetlookup");
		return sc.getOffsetsBefore(request);
	}

	@Override
	public void readEvent(final String topicId, final String partitionId) {
		final org.apache.kafka.common.TopicPartition tp = new org.apache.kafka.common.TopicPartition(topicId,
				Integer.parseInt(partitionId));
		// final Consumer<String, String> consumerForPartition =
		// factory.getConsumerFor(tp);
		// FIXME: read from kafka
		// consumerForPartition.poll(KAFKA_READ_TIMEOUT_MS);
		// consumerForPartition.close();

	}
}

@Component
class Factory {

	@Autowired
	@Qualifier("kafkaBrokers")
	private String kafkaAddress;

	@Autowired
	@Qualifier("zookeeperBrokers")
	private String zookeeperAddress;

	@Autowired
	private Producer<String, String> producer;

	@Autowired
	private Consumer<String, String> consumer;

	public Factory() {
	}

	@Bean
	public Producer<String, String> createProducer() {
		return new KafkaProducer(getProps());
	}

	@Bean
	public Consumer<String, String> createConsumer() {
		return new KafkaConsumer<>(getProps());
	}

	private Properties getProps() {
		final Properties props = new Properties();
		props.put("metadata.broker.list", zookeeperAddress);
		props.put("bootstrap.servers", kafkaAddress);
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		return props;
	}

	public Producer<String, String> getProducer() {
		return producer;
	}

	public Consumer<String, String> getConsumer() {
		return consumer;
	}

	public SimpleConsumer getSimpleConsumer() {
		final String[] split = kafkaAddress.split(":");
		return new SimpleConsumer(split[0], Integer.parseInt(split[1]), 1000, 64 * 1024, "leaderlookup");
	}

}