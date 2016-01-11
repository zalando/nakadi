package de.zalando.aruha.nakadi.domain;

import com.google.common.collect.Lists;

import java.util.Collections;
import java.util.List;

public class ClientTopology {

    private final String clientId;

    private final List<TopicPartition> partitions;

    private final boolean applied;

    public ClientTopology(final String id, final List<TopicPartition> partitions, final boolean applied) {
        this.clientId = id;
        this.partitions = Lists.newArrayList(partitions);
        this.applied = applied;
    }

    public String getClientId() {
        return clientId;
    }

    public List<TopicPartition> getPartitions() {
        return Collections.unmodifiableList(partitions);
    }

    public boolean isApplied() {
        return applied;
    }
}
