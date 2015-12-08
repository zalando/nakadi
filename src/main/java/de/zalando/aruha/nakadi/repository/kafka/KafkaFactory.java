package de.zalando.aruha.nakadi.repository.kafka;

import kafka.javaapi.consumer.SimpleConsumer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;

import java.util.Properties;

@Component
@PropertySource("${nakadi.config}")
class KafkaFactory {

	@Value("${nakadi.kafka.broker}")
	private String kafkaAddress;

	@Value("${nakadi.zookeeper.brokers}")
	private String zookeeperAddress;

	@Autowired
	private Producer<String, String> producer;
	@Autowired
	private Consumer<String, String> consumer;

	public KafkaFactory() {
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
