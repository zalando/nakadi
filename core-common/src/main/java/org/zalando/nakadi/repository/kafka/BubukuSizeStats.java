package org.zalando.nakadi.repository.kafka;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

public class BubukuSizeStats {
    public static class TotalStats {
        private final long usedKb;
        private final long freeKb;

        public TotalStats(
                @JsonProperty("used_kb") final long usedKb,
                @JsonProperty("free_kb") final long freeKb) {
            this.usedKb = usedKb;
            this.freeKb = freeKb;
        }

        public long getUsedKb() {
            return usedKb;
        }

        public long getFreeKb() {
            return freeKb;
        }
    }

    private final TotalStats totalStats;
    private final Map<String, Map<String, Long>> perPartitionStats;

    public BubukuSizeStats(
            @JsonProperty("disk") final TotalStats totalStats,
            @JsonProperty("topics") final Map<String, Map<String, Long>> perPartitionStats) {
        this.totalStats = totalStats;
        this.perPartitionStats = perPartitionStats;
    }

    public TotalStats getTotalStats() {
        return totalStats;
    }

    public Map<String, Map<String, Long>> getPerPartitionStats() {
        return perPartitionStats;
    }
}
