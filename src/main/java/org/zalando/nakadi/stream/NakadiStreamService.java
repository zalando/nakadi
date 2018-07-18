package org.zalando.nakadi.stream;


import com.google.common.base.Charsets;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.repository.EventTypeRepository;
import org.zalando.nakadi.service.timeline.TimelineService;

import java.security.MessageDigest;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Component
public class NakadiStreamService {

    private static final Logger LOG = LoggerFactory.getLogger(NakadiStreamService.class);
    private final EventTypeRepository eventTypeRepository;
    private final TimelineService timelineService;
    private final ConcurrentHashMap<String, KafkaStreams> idsStreams;

    @Autowired
    public NakadiStreamService(final EventTypeRepository eventTypeRepository,
                               final TimelineService timelineService) {
        this.eventTypeRepository = eventTypeRepository;
        this.timelineService = timelineService;
        this.idsStreams = new ConcurrentHashMap<>();
    }

    public String start(final StreamConfig streamConfig) {
        final List<EventType> eventTypes = eventTypeRepository.list();
        final List<Timeline> timelines = eventTypes.stream()
                .filter(eventType -> streamConfig.getFromEventTypes().contains(eventType.getName()))
                .map(et -> timelineService.getActiveTimeline(et))
                .collect(Collectors.toList());

        if (timelines.isEmpty()) {
            throw new RuntimeException("Event types were not found");
        }

        final Timeline outputTimeline = eventTypes.stream()
                .filter(eventType -> streamConfig.getToEventType().equals(eventType.getName()))
                .findFirst()
                .map(eventType -> timelineService.getActiveTimeline(eventType))
                .orElseThrow(() -> new RuntimeException("Output event type does not exist"));

        final KStreamBuilder kStreamBuilder = new KStreamBuilder();
        final KTable<String, String> streamA = kStreamBuilder.table(timelines.get(0).getTopic(), timelines.get(0).getEventType());
        final KTable<String, String> streamB = kStreamBuilder.table(timelines.get(1).getTopic(), timelines.get(1).getEventType());
        streamA.outerJoin(streamB, (v1, v2) -> "{{" + v1 + "}, {" + v2 + "}}").to(outputTimeline.getTopic());

        // it is impossible to have timelines !!!
        final KafkaStreams streams = new KafkaStreams(kStreamBuilder,
                timelineService.getTopicRepository(outputTimeline)
                        .getStreamingProperties(streamConfig.getApplicationId()));
        streams.start();
        final String streamId = getStreamId(streamConfig.getApplicationId());
        idsStreams.putIfAbsent(streamId, streams);
        return streamId;
    }

    public void stop(final String streamId) {
        final KafkaStreams streams = idsStreams.get(streamId);
        if (streams == null) {
            throw new IllegalStateException("Failed to find runnig stream " + streamId);
        }
        streams.close();
    }

    private String getStreamId(final String applicationId) {
        final MessageDigest messageDigest = DigestUtils.getSha256Digest();
        messageDigest.reset();
        messageDigest.update(applicationId.getBytes(Charsets.UTF_8));
        messageDigest.update(Long.toString(System.currentTimeMillis()).getBytes(Charsets.UTF_8));
        return Hex.encodeHexString(messageDigest.digest());
    }

}
