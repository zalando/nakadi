package de.zalando.aruha.nakadi.service;

import de.zalando.aruha.nakadi.domain.TopicPartition;

import java.util.List;
import java.util.Map;

public interface PartitionDistributor {

    /**
     * Distribute partitions for the specified client indexes
     *
     * @param subscriptionId id of subscription
     * @param clientIndexes the indexes of the clients
     * @param clientsNum total number of clients
     * @return partition distribution for specified clients indexes
     */
    Map<Integer, List<TopicPartition>> getPartitionsForClients(String subscriptionId,
                                                               List<Integer> clientIndexes,
                                                               int clientsNum);
}
