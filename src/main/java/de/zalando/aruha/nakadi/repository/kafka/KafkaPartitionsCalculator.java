package de.zalando.aruha.nakadi.repository.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;
import java.util.stream.Stream;

public class KafkaPartitionsCalculator {
    // Contains mapping from message size (in bytes) to list of throughput of kafka with (index + 1) as partition count.
    private final NavigableMap<Integer, float[]> stats = new TreeMap<>();

    private KafkaPartitionsCalculator(InstanceInfo instanceInfo) {
        for (SpeedStatistics ss : instanceInfo.getStats()) {
            stats.put(ss.getMessageSize(), ss.getSpeed());
        }
    }

    public int getBestPartitionsCount(int messageSize, float mbsPerSecond) {
        final Map.Entry<Integer, float[]> floor = stats.floorEntry(messageSize);
        final Map.Entry<Integer, float[]> ceil = stats.ceilingEntry(messageSize);
        if (floor == null) {
            return getBestPartitionsCount(ceil.getValue(), mbsPerSecond);
        } else if (ceil == null) {
            return getBestPartitionsCount(floor.getValue(), mbsPerSecond);
        } else {
            final int floorResult = getBestPartitionsCount(floor.getValue(), mbsPerSecond);
            if (Objects.equals(floor.getKey(), ceil.getKey())) {
                return floorResult;
            }
            final int ceilResult = getBestPartitionsCount(ceil.getValue(), mbsPerSecond);
            return floorResult + (ceilResult - floorResult) * (messageSize - floor.getKey()) / (ceil.getKey() - floor.getKey());
        }
    }

    private static int getBestPartitionsCount(float[] perPartitionThroughput, float mbsPerSecond) {
        int nearestIndex = -1;
        for (int i = 0; i < perPartitionThroughput.length; ++i) {
            if (mbsPerSecond <= perPartitionThroughput[i]) {
                return i + 1;
            }
            if (nearestIndex == -1 || perPartitionThroughput[nearestIndex] < perPartitionThroughput[i]) {
                nearestIndex = i;
            }
        }
        return nearestIndex + 1;
    }

    private static final String PARTITION_STATISTICS = "/partitions_statistics.json";

    private static class SpeedStatistics {
        private int messageSize;
        private float[] speed;

        public int getMessageSize() {
            return messageSize;
        }

        public void setMessageSize(int messageSize) {
            this.messageSize = messageSize;
        }

        public float[] getSpeed() {
            return speed;
        }

        public void setSpeed(float[] speed) {
            this.speed = speed;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            SpeedStatistics that = (SpeedStatistics) o;

            if (messageSize != that.messageSize) return false;
            return Arrays.equals(speed, that.speed);

        }

        @Override
        public int hashCode() {
            return messageSize;
        }
    }

    private static class InstanceInfo {
        private String name;
        private SpeedStatistics[] stats;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public SpeedStatistics[] getStats() {
            return stats;
        }

        public void setStats(SpeedStatistics[] stats) {
            this.stats = stats;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            InstanceInfo that = (InstanceInfo) o;

            if (!name.equals(that.name)) return false;
            return Arrays.equals(stats, that.stats);

        }

        @Override
        public int hashCode() {
            return name.hashCode();
        }
    }

    static KafkaPartitionsCalculator load(final ObjectMapper objectMapper, final String instanceType) throws IOException {
        try (final InputStream in = KafkaPartitionsCalculator.class.getResourceAsStream(PARTITION_STATISTICS)) {
            if (null == in) {
                throw new IOException("Resource with name " + PARTITION_STATISTICS + " is not found");
            }
            return load(objectMapper, instanceType, in);
        }
    }

    @VisibleForTesting
    static KafkaPartitionsCalculator load(ObjectMapper objectMapper, String instanceType, InputStream in) throws IOException {
        final InstanceInfo[] instanceInfos = objectMapper.readValue(in, InstanceInfo[].class);
        final InstanceInfo instanceInfo = Stream.of(instanceInfos)
                .filter(ii -> instanceType.equals(ii.getName()))
                .findAny().orElseThrow(() -> new IllegalArgumentException("Failed to find instance " + instanceType + " configuration"));
        return new KafkaPartitionsCalculator(instanceInfo);
    }

}
