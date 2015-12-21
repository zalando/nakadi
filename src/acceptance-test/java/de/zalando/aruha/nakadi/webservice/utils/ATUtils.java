package de.zalando.aruha.nakadi.webservice.utils;

import de.zalando.aruha.nakadi.domain.TopicPartition;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import java.util.List;

public class ATUtils {

    private ATUtils() {
    }

    public static List getLatestOffsets(final String topic) {
        final RestTemplate restTemplate = new RestTemplate();
        final String url = "http://localhost:8080/topics/" + topic + "/partitions";
        final ResponseEntity<List> offset = restTemplate.getForEntity(url, List.class);
        return offset.getBody();
    }
}
