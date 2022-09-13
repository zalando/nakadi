package org.zalando.nakadi.config;

public class Configuration {

  private String apiUrl;
  private String zookeeperUrl;
  private Kafka kafka;
  private Database database;
  private EventTypeDeletableSubscription eventTypeDeletableSubscription;
  private Subscription subscription;
  private Stream stream;
  private KpiConfig kpiConfig;

  public Configuration() {
  }

  public String getApiUrl() {
    return apiUrl;
  }

  public void setApiUrl(String apiUrl) {
    this.apiUrl = apiUrl;
  }

  public Kafka getKafka() {
    return kafka;
  }

  public void setKafka(Kafka kafka) {
    this.kafka = kafka;
  }

  public String getZookeeperUrl() {
    return zookeeperUrl;
  }

  public void setZookeeperUrl(String zookeeperUrl) {
    this.zookeeperUrl = zookeeperUrl;
  }

  public Database getDatabase() {
    return database;
  }

  public void setDatabase(Database database) {
    this.database = database;
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

  public static class Database {
    private String url;
    private String username;
    private String password;

    public String getUrl() {
      return url;
    }

    public void setUrl(String url) {
      this.url = url;
    }

    public String getUsername() {
      return username;
    }

    public void setUsername(String username) {
      this.username = username;
    }

    public String getPassword() {
      return password;
    }

    public void setPassword(String password) {
      this.password = password;
    }
  }

  public static class Kafka {
    private String bootstrapServers;
    private int minInSyncReplicas;
    private String securityProtocol;
    private String saslMechanism;
    private String saslJaasConfig;
    private String saslClientCallbackHandlerClass;

    public String getBootstrapServers() {
      return bootstrapServers;
    }

    public void setBootstrapServers(String bootstrapServers) {
      this.bootstrapServers = bootstrapServers;
    }

    public int getMinInSyncReplicas() {
      return minInSyncReplicas;
    }

    public void setMinInSyncReplicas(int minInSyncReplicas) {
      this.minInSyncReplicas = minInSyncReplicas;
    }

    public String getSecurityProtocol() {
      return securityProtocol;
    }

    public void setSecurityProtocol(String securityProtocol) {
      this.securityProtocol = securityProtocol;
    }

    public String getSaslMechanism() {
      return saslMechanism;
    }

    public void setSaslMechanism(String saslMechanism) {
      this.saslMechanism = saslMechanism;
    }

    public String getSaslJaasConfig() {
      return saslJaasConfig;
    }

    public void setSaslJaasConfig(String saslJaasConfig) {
      this.saslJaasConfig = saslJaasConfig;
    }

    public String getSaslClientCallbackHandlerClass() {
      return saslClientCallbackHandlerClass;
    }

    public void setSaslClientCallbackHandlerClass(String saslClientCallbackHandlerClass) {
      this.saslClientCallbackHandlerClass = saslClientCallbackHandlerClass;
    }
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
