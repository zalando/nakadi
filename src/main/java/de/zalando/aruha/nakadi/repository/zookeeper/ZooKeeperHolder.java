package de.zalando.aruha.nakadi.repository.zookeeper;

import java.io.IOException;

import javax.annotation.PostConstruct;

import org.apache.zookeeper.ZooKeeper;

import org.springframework.beans.factory.annotation.Value;

import org.springframework.context.annotation.PropertySource;

import org.springframework.stereotype.Component;

@Component
@PropertySource("${nakadi.config}")
public class ZooKeeperHolder {
	@Value("${nakadi.zookeeper.brokers}")
	private String brokers;
	private ZooKeeper zooKeeper;

	@PostConstruct
	public void init() throws IOException {
		zooKeeper = new ZooKeeper(brokers, 30000, null);
	}

	public ZooKeeper get() throws IOException {
		return zooKeeper;
	}
}
