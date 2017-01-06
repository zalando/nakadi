package org.zalando.nakadi.repository.kafka;

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
import org.zalando.nakadi.config.NakadiSettings;
import org.zalando.nakadi.domain.EventTypeStatistics;

public class PartitionsCalculator {
    // Contains mapping from message size (in bytes) to list of throughput of kafka with (index + 1) as partition count.
    private final NavigableMap<Integer, float[]> stats = new TreeMap<>();
    private final int maxPartitionCount;
    private final int defaultPartitionCount;

    private PartitionsCalculator(final InstanceInfo instanceInfo,
                                 final int defaultPartitionCount,
                                 final int maxPartitionCount) {
        this.defaultPartitionCount = defaultPartitionCount;
        this.maxPartitionCount = maxPartitionCount;
        for (final SpeedStatistics ss : instanceInfo.getStats()) {
            stats.put(ss.getMessageSize(), ss.getSpeed());
        }
    }

    public int getBestPartitionsCount(final EventTypeStatistics stat) {
        if (null == stat) {
            return defaultPartitionCount;
        }
        final int maxPartitionsDueParallelism = Math.max(stat.getReadParallelism(), stat.getWriteParallelism());
        if (maxPartitionsDueParallelism >= maxPartitionCount) {
            return maxPartitionCount;
        }
        return Math.min(maxPartitionCount, Math.max(
                maxPartitionsDueParallelism,
                calculatePartitionsAccordingLoad(stat.getMessagesPerMinute(), stat.getMessageSize())));

    }

    private int calculatePartitionsAccordingLoad(final int messagesPerMinute, final int avgEventSizeBytes) {
        final float throughoutputMbPerSec = ((float) messagesPerMinute * (float) avgEventSizeBytes)
                / (1024.f * 1024.f * 60.f);
        return getBestPartitionsCount(avgEventSizeBytes, throughoutputMbPerSec);
    }

    public int getBestPartitionsCount(final int messageSize, final float mbsPerSecond) {
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
            return floorResult + ((ceilResult - floorResult) * (messageSize - floor.getKey())) / (ceil.getKey()
                    - floor.getKey());
        }
    }

    private static int getBestPartitionsCount(final float[] perPartitionThroughput, final float mbsPerSecond) {
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

        public void setMessageSize(final int messageSize) {
            this.messageSize = messageSize;
        }

        public float[] getSpeed() {
            return speed;
        }

        public void setSpeed(final float[] speed) {
            this.speed = speed;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            final SpeedStatistics that = (SpeedStatistics) o;
            return messageSize == that.messageSize && Arrays.equals(speed, that.speed);
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

        public void setName(final String name) {
            this.name = name;
        }

        public SpeedStatistics[] getStats() {
            return stats;
        }

        public void setStats(final SpeedStatistics[] stats) {
            this.stats = stats;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            final InstanceInfo that = (InstanceInfo) o;
            return name.equals(that.name) && Arrays.equals(stats, that.stats);
        }

        @Override
        public int hashCode() {
            return name.hashCode();
        }
    }

    static PartitionsCalculator load(final ObjectMapper objectMapper, final String instanceType,
                                     final NakadiSettings nakadiSettings)
            throws IOException {
        try (final InputStream in = PartitionsCalculator.class.getResourceAsStream(PARTITION_STATISTICS)) {
            if (null == in) {
                throw new IOException("Resource with name " + PARTITION_STATISTICS + " is not found");
            }
            return load(objectMapper, instanceType, in, nakadiSettings.getDefaultTopicPartitionCount(),
                    nakadiSettings.getMaxTopicPartitionCount());
        }
    }

    @VisibleForTesting
    static PartitionsCalculator load(final ObjectMapper objectMapper, final String instanceType,
                                     final InputStream in, final int defaultPartitionsCount,
                                     final int maxPartitionCount) throws IOException {
        final InstanceInfo[] instanceInfos = objectMapper.readValue(in, InstanceInfo[].class);
        final InstanceInfo instanceInfo = Stream.of(instanceInfos)
                .filter(ii -> instanceType.equals(ii.getName()))
                .findAny().orElseThrow(() -> new IllegalArgumentException("Failed to find instance " + instanceType
                        + " configuration"));
        return new PartitionsCalculator(instanceInfo, defaultPartitionsCount, maxPartitionCount);
    }

}
