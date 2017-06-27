package org.zalando.nakadi.partitioning;

import com.google.common.collect.ImmutableMap;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

@Component
public class HashPartitionStrategyCrutch {

    private static final String PROPERTY_PREFIX = "nakadi.hashPartitioning.overrideOrder";

    // due to initial problem with order of partitions we need to keep the original order now
    private final Map<Integer, List<Integer>> partitionsOrder;

    @Autowired
    @SuppressWarnings("unchecked")
    public HashPartitionStrategyCrutch(final Environment environment,
                                       @Value("${" + PROPERTY_PREFIX + ".max}") final int maxPartitionNum) {

        final ImmutableMap.Builder<Integer, List<Integer>> mapBuilder = ImmutableMap.builder();
        for (int i = 1; i <= maxPartitionNum; i++) {
            final List predefinedOrder = environment.getProperty(PROPERTY_PREFIX + ".p" + i, List.class);
            if (predefinedOrder != null) {
                mapBuilder.put(i, predefinedOrder);
            }
        }
        partitionsOrder = mapBuilder.build();
    }

    public int adjustPartitionIndex(final int index, final int partitionNum) {
        return partitionsOrder.containsKey(partitionNum) ? partitionsOrder.get(partitionNum).get(index) : index;
    }

}
