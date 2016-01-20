package de.zalando.aruha.nakadi.webservice.utils;

import static java.text.MessageFormat.format;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import org.springframework.web.client.RestTemplate;

public class TestHelper {

    private final String baseUrl;
    private final RestTemplate restTemplate;

    public TestHelper(final String baseUrl) {
        this.baseUrl = baseUrl;
        restTemplate = new RestTemplate();
    }

    public boolean createSubscription(final String subscription, final List<String> topics) {
        String url = format("{0}/subscriptions/{1}", baseUrl, subscription);
        final ResponseEntity<Object> response = restTemplate.postForEntity(url, topics, Object.class);
        return response != null && response.getStatusCode() == HttpStatus.CREATED;
    }

    @SuppressWarnings("unchecked")
    public List<Map<String, String>> getLatestOffsets(final String topic) {
        final String url = format("{0}/topics/{1}/partitions", baseUrl, topic);
        final ResponseEntity<List> offset = restTemplate.getForEntity(url, List.class);
        return offset.getBody();
    }

    public boolean pushEventToPartition(final String topic, final String partition, final Object event) {
        String url = format("{0}/topics/{1}/partitions/{2}/events", baseUrl, topic, partition);
        final ResponseEntity<Object> responseEntity = restTemplate.postForEntity(url, event, Object.class);
        return responseEntity != null && responseEntity.getStatusCode() == HttpStatus.CREATED;
    }

    public void pushEventsToPartition(final String topic, final String partition, final Object event,
            final int eventNum) {
        for (int i = 0; i < eventNum; i++) {
            pushEventToPartition(topic, partition, event);
        }
    }

    public Optional<String> getOffsetForPartition(final List<Map<String, String>> allOffsets, final String partition) {
        return allOffsets.stream().filter(partitionInfo ->
                                 partition.equals(partitionInfo.get("partition"))).findFirst().map(partitionInfo ->
                                 partitionInfo.get("newestAvailableOffset"));
    }
}
