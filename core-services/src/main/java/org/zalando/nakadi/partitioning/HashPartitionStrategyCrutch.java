package org.zalando.nakadi.partitioning;

import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Component
public class HashPartitionStrategyCrutch {

    private static final Logger LOG = LoggerFactory.getLogger(HashPartitionStrategyCrutch.class);

    private static final String PROPERTY_PREFIX = "nakadi.hashPartitioning.overrideOrder";

    // due to initial problem with order of partitions we need to keep the original order now
    private final Map<Integer, List<Integer>> partitionsOrder;

    @Autowired
    @SuppressWarnings("unchecked")
    public HashPartitionStrategyCrutch(final Environment environment,
                                       @Value("${" + PROPERTY_PREFIX + ".max:0}") final int maxPartitionNum) {

        final ImmutableMap.Builder<Integer, List<Integer>> mapBuilder = ImmutableMap.builder();
        for (int pCount = 1; pCount <= maxPartitionNum; pCount++) {

            final String propertyName = PROPERTY_PREFIX + ".p" + pCount;
            final List<String> predefinedOrder = (List<String>) environment.getProperty(propertyName, List.class);

            if (predefinedOrder != null) {
                final List<Integer> predefinedOrderInt = predefinedOrder.stream()
                        .map(Integer::parseInt)
                        .collect(Collectors.toList());

                // check that element count equals to number of partitions
                if (pCount != predefinedOrder.size()) {
                    throw new IllegalArgumentException(propertyName + " property has wrong count of elements");
                }

                // check that there is not index that is out of bounds
                final int partitionMaxIndex = pCount - 1;
                final boolean indexOutOfBouns = predefinedOrderInt.stream()
                        .anyMatch(index -> index > partitionMaxIndex || index < 0);
                if (indexOutOfBouns) {
                    throw new IllegalArgumentException(propertyName + " property has wrong partition index");
                }

                mapBuilder.put(pCount, predefinedOrderInt);
            }
        }
        partitionsOrder = mapBuilder.build();

        LOG.info("Initialized partitions override map with {} values:", partitionsOrder.size());
        partitionsOrder.forEach((partitionCount, order) -> LOG.info("{}: {}", partitionCount, order));
    }

    public int adjustPartitionIndex(final int index, final int partitionNum) {
        return partitionsOrder.containsKey(partitionNum) ? partitionsOrder.get(partitionNum).get(index) : index;
    }

}
