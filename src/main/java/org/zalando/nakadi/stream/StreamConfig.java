package org.zalando.nakadi.stream;

import java.io.OutputStream;
import java.util.List;
import java.util.Properties;
import java.util.Set;

public final class StreamConfig {

    private Set<String> eventTypes;
    private List<String> expressions;
    private OutputStream outputStream;
    private String outputEventType;
    private String outputTopic;
    private String[] topics;
    private Properties kafkaStreamProperties;

    private StreamConfig() {
    }

    public static StreamConfig newStreamConfig() {
        return new StreamConfig();
    }

    public StreamConfig setEventTypes(final Set<String> eventTypes) {
        this.eventTypes = eventTypes;
        return this;
    }

    public StreamConfig setExpressions(final List<String> expressions) {
        this.expressions = expressions;
        return this;
    }

    public StreamConfig setOutputStream(final OutputStream outputStream) {
        this.outputStream = outputStream;
        return this;
    }

    public StreamConfig setOutputEventType(final String outputEventType) {
        this.outputEventType = outputEventType;
        return this;
    }

    public StreamConfig setTopics(final String[] topics) {
        this.topics = topics;
        return this;
    }

    public StreamConfig setOutputTopic(final String outputTopic) {
        this.outputTopic = outputTopic;
        return this;
    }

    public StreamConfig setKafkaStreamProperties(final Properties kafkaStreamProperties) {
        this.kafkaStreamProperties = kafkaStreamProperties;
        return this;
    }

    public Set<String> getEventTypes() {
        return eventTypes;
    }

    public List<String> getExpressions() {
        return expressions;
    }

    public OutputStream getOutputStream() {
        return outputStream;
    }


    public String getOutputEventType() {
        return outputEventType;
    }

    public boolean isToEventType() {
        return outputEventType != null;
    }

    public String getOutputTopic() {
        return outputTopic;
    }

    public String[] getTopics() {
        return topics;
    }

    public Properties getKafkaStreamProperties() {
        return kafkaStreamProperties;
    }
}
