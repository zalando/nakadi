package org.zalando.nakadi.service.subscription;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.exceptions.InternalNakadiException;
import org.zalando.nakadi.exceptions.NakadiException;
import org.zalando.nakadi.exceptions.NakadiRuntimeException;
import org.zalando.nakadi.exceptions.NoSuchEventTypeException;
import org.zalando.nakadi.repository.TopicRepository;
import org.zalando.nakadi.repository.kafka.KafkaTopicRepository;
import org.zalando.nakadi.service.CursorConverter;
import org.zalando.nakadi.service.subscription.model.Partition;
import org.zalando.nakadi.service.timeline.TimelineService;

public class KafkaClient {

    private final Subscription subscription;
    private final TimelineService timelineService;
    private final CursorConverter cursorConverter;
    private final TopicRepository topicRepository;

    public KafkaClient(final Subscription subscription, final TimelineService timelineService,
                       final CursorConverter cursorConverter)
            throws InternalNakadiException, NoSuchEventTypeException {
        this.subscription = subscription;
        this.timelineService = timelineService;
        this.cursorConverter = cursorConverter;
        // FIXME TIMELINE: for refactoring purposes, has to be removed during timeline event consumption task
        // for now we always will have the same topic repo, KafkaTopicRepository
        this.topicRepository = timelineService.getTopicRepository(
                timelineService.getActiveTimelinesOrdered(subscription.getEventTypes().iterator().next()).get(0));
        // FIXME TIMELINE: for refactoring purposes, has to be removed during timeline event consumption task
    }

    public Map<Partition.PartitionKey, NakadiCursor> getSubscriptionOffsets() {
        try {
            final List<NakadiCursor> existingPositions = new ArrayList<>();
            switch (subscription.getReadFrom()) {
                case BEGIN:
                    for (final String eventType : subscription.getEventTypes()) {
                        final List<Timeline> activeTimelines = timelineService.getActiveTimelinesOrdered(eventType);
                        final Timeline timeline = activeTimelines.get(0);
                        timelineService.getTopicRepository(timeline)
                                .loadTopicStatistics(Collections.singletonList(timeline))
                                .forEach(stat -> existingPositions.add(stat.getBeforeFirst()));
                    }
                    break;
                case END:
                    for (final String eventType : subscription.getEventTypes()) {
                        final List<Timeline> activeTimelines = timelineService.getActiveTimelinesOrdered(eventType);
                        final Timeline timeline = activeTimelines.get(activeTimelines.size() - 1);
                        timelineService.getTopicRepository(timeline)
                                .loadTopicStatistics(Collections.singletonList(timeline))
                                .forEach(stat -> existingPositions.add(stat.getLast()));
                    }
                    break;
                case CURSORS:
                    subscription.getInitialCursors().forEach(cursor -> {
                        try {
                            existingPositions.add(cursorConverter.convert(cursor));
                        } catch (final Exception e) {
                            throw new NakadiRuntimeException(e);
                        }
                    });
                    break;
                default:
                    throw new IllegalStateException("unsupported start position " + subscription.getReadFrom() +
                            " for subscription " + subscription.getId());
            }
            return existingPositions.stream().collect(
                    Collectors.toMap(
                            cursor -> new Partition.PartitionKey(cursor.getTopic(), cursor.getPartition()),
                            cursor -> cursor
                    ));
        } catch (final NakadiException e) {
            throw new NakadiRuntimeException(e);
        }
    }

    public org.apache.kafka.clients.consumer.Consumer<String, String> createKafkaConsumer() {
        // TODO: Refactor to use correct layering
        return ((KafkaTopicRepository) topicRepository).createKafkaConsumer();
    }
}
