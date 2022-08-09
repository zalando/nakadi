package org.zalando.nakadi.config;

public class Configuration {

  private EventTypeDeletableSubscription eventTypeDeletableSubscription;
  private Subscription subscription;
  private Stream stream;
  private KpiConfig kpiConfig;

  public Configuration() {
  }

  public Subscription getSubscription() {
    return subscription;
  }

  public void setSubscription(Subscription subscription) {
    this.subscription = subscription;
  }

  public EventTypeDeletableSubscription getEventTypeDeletableSubscription() {
    return eventTypeDeletableSubscription;
  }

  public void setEventTypeDeletableSubscription(
      EventTypeDeletableSubscription eventTypeDeletableSubscription) {
    this.eventTypeDeletableSubscription = eventTypeDeletableSubscription;
  }

  public Stream getStream() {
    return stream;
  }

  public void setStream(Stream stream) {
    this.stream = stream;
  }

  public KpiConfig getKpiConfig() {
    return kpiConfig;
  }

  public void setKpiConfig(KpiConfig kpiConfig) {
    this.kpiConfig = kpiConfig;
  }

 public static class Stream {
    public long maxStreamMemoryBytes;
    public long maxCommitTimeout;

    public long getMaxStreamMemoryBytes() {
      return maxStreamMemoryBytes;
    }

    public void setMaxStreamMemoryBytes(long maxStreamMemoryBytes) {
      this.maxStreamMemoryBytes = maxStreamMemoryBytes;
    }

    public long getMaxCommitTimeout() {
      return maxCommitTimeout;
    }

    public void setMaxCommitTimeout(long maxCommitTimeout) {
      this.maxCommitTimeout = maxCommitTimeout;
    }
  }

  public static class Subscription {
    public int maxPartitions;

    public int getMaxPartitions() {
      return maxPartitions;
    }

    public void setMaxPartitions(int maxPartitions) {
      this.maxPartitions = maxPartitions;
    }
  }

  public static class KpiConfig {
    public long streamDataCollectionFrequencyMs;

    public long getStreamDataCollectionFrequencyMs() {
      return streamDataCollectionFrequencyMs;
    }

    public void setStreamDataCollectionFrequencyMs(long streamDataCollectionFrequencyMs) {
      this.streamDataCollectionFrequencyMs = streamDataCollectionFrequencyMs;
    }
  }

  public static class EventTypeDeletableSubscription {
    private String owningApplication;
    private String consumerGroup;

    public String getOwningApplication() {
      return owningApplication;
    }

    public void setOwningApplication(String owningApplication) {
      this.owningApplication = owningApplication;
    }

    public String getConsumerGroup() {
      return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
      this.consumerGroup = consumerGroup;
    }
  }

}
