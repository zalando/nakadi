package org.zalando.nakadi.service.timeline;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zalando.nakadi.domain.ConsumedEvent;
import org.zalando.nakadi.domain.EventTypePartition;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.PartitionStatistics;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.domain.TopicPartition;
import org.zalando.nakadi.exceptions.runtime.InvalidCursorException;
import org.zalando.nakadi.exceptions.runtime.NakadiRuntimeException;
import org.zalando.nakadi.exceptions.runtime.ServiceTemporarilyUnavailableException;
import org.zalando.nakadi.repository.EventConsumer;
import org.zalando.nakadi.repository.TopicRepository;
import org.zalando.nakadi.repository.kafka.KafkaFactory;
import org.zalando.nakadi.util.NakadiCollectionUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

public class MultiTimelineEventConsumer implements EventConsumer.ReassignableEventConsumer {
    private static final Logger LOG = LoggerFactory.getLogger(MultiTimelineEventConsumer.class);
    private final String clientId;
    /**
     * Contains latest offsets that were sent to client of this class
     */
    private final Map<EventTypePartition, NakadiCursor> latestOffsets = new HashMap<>();
    /**
     * Active timelines list for consumed event types
     */
    private final Map<String, List<Timeline>> eventTypeTimelines = new HashMap<>();
    /**
     * Mapping from event type name to listener that is used to listen for timeline changes
     */
    private final Map<String, TimelineSync.ListenerRegistration> timelineRefreshListeners = new HashMap<>();
    /**
     * Mapping from topic repository to event consumer that was created to consume events from this topic repository.
     */
    private final Map<TopicRepository, EventConsumer.LowLevelConsumer> eventConsumers = new HashMap<>();
    /**
     * Offsets, that should trigger election of topic repository for next timeline. (Actually - map of latest offsets
     * for each event type partition within current timeline.
     */
    private final Map<EventTypePartition, String> borderOffsets = new HashMap<>();
    private final TimelineService timelineService;
    private final TimelineSync timelineSync;
    private final AtomicBoolean timelinesChanged = new AtomicBoolean(false);
    private final Comparator<NakadiCursor> comparator;

    public MultiTimelineEventConsumer(
            final String clientId,
            final TimelineService timelineService,
            final TimelineSync timelineSync,
            final Comparator<NakadiCursor> comparator) {
        this.clientId = clientId;
        this.timelineService = timelineService;
        this.timelineSync = timelineSync;
        this.comparator = comparator;
    }

    @Override
    public Set<EventTypePartition> getAssignment() {
        return latestOffsets.keySet();
    }

    @Override
    public List<ConsumedEvent> readEvents() {
        if (timelinesChanged.compareAndSet(true, false)) {
            try {
                onTimelinesChanged();
            } catch (final InvalidCursorException ex) {
                throw new NakadiRuntimeException(ex);
            }
        }
        final List<ConsumedEvent> result;
        try {
            result = poll();
        } catch (KafkaFactory.KafkaCrutchException kce) {
            LOG.warn("Kafka connections should be reinitialized because consumers should be recreated", kce);
            final List<NakadiCursor> tmpOffsets = new ArrayList<>(latestOffsets.values());
            // close all the clients
            reassign(Collections.emptyList());
            // create new clients
            reassign(tmpOffsets);
            return Collections.emptyList();
        }
        if (result.isEmpty()) {
            return result;
        }

        final List<ConsumedEvent> filteredResult = new ArrayList<>(result.size());
        for (final ConsumedEvent event : result) {
            final EventTypePartition etp = event.getPosition().getEventTypePartition();
            latestOffsets.put(etp, event.getPosition());
            final String border = borderOffsets.get(etp);
            final boolean timelineBorderReached = null != border
                    && border.compareTo(event.getPosition().getOffset()) <= 0;
            if (timelineBorderReached) {
                timelinesChanged.set(true);
            }
            // Here we are trying to avoid the null events
            if (event.getEvent() != null) {
                filteredResult.add(event);
            }
        }
        return filteredResult;
    }

    /**
     * Gets data from current event consumers. It tries to use as less list allocations as it is possible.
     *
     * @return List of consumed events.
     */
    private List<ConsumedEvent> poll() {
        List<ConsumedEvent> result = null;
        boolean newCollectionCreated = false;
        for (final EventConsumer consumer : eventConsumers.values()) {
            final List<ConsumedEvent> partialResult = consumer.readEvents();
            if (null == result) {
                result = partialResult;
            } else {
                if (!newCollectionCreated) {
                    result = new ArrayList<>(result);
                    newCollectionCreated = true;
                }
                result.addAll(partialResult);
            }
        }
        return result == null ? Collections.emptyList() : result;
    }

    private TopicRepository selectCorrectTopicRepo(
            final NakadiCursor cursor,
            final Consumer<NakadiCursor> cursorReplacer,
            final Consumer<NakadiCursor> lastTimelinePosition)
            throws ServiceTemporarilyUnavailableException {
        final List<Timeline> eventTimelines = eventTypeTimelines.get(cursor.getEventType());
        final ListIterator<Timeline> itTimeline = eventTimelines.listIterator(eventTimelines.size());
        // select last timeline, and then move back until position was found.
        Timeline electedTimeline = itTimeline.previous();
        while (itTimeline.hasPrevious()) {
            final Timeline toCheck = itTimeline.previous();
            final NakadiCursor latest = toCheck.calculateNakadiLatestPosition(cursor.getPartition());
            if (latest == null) {
                electedTimeline = toCheck;
            } else if (comparator.compare(latest, cursor) > 0) {
                // There is a border case - latest is equal to begin (that means that there are no available events
                // there), and one should position on timeline that have something inside.
                final NakadiCursor firstItem = timelineService.getTopicRepository(toCheck)
                        .loadPartitionStatistics(toCheck, cursor.getPartition())
                        .get().getFirst();
                if (comparator.compare(latest, firstItem) >= 0) {
                    electedTimeline = toCheck;
                } else {
                    LOG.info("Timeline {} is empty, skipping", toCheck);
                }
            } else {
                break;
            }
        }
        final TopicRepository result = timelineService.getTopicRepository(electedTimeline);
        if (electedTimeline.getOrder() != cursor.getTimeline().getOrder()) {
            // It seems that cursor jumped to different timeline. One need to fetch very first cursor in timeline.
            final NakadiCursor replacement = getBeforeFirstCursor(result, electedTimeline, cursor.getPartition());
            LOG.info("Replacing cursor because of jumping between timelines from {} to {}", cursor, replacement);
            cursorReplacer.accept(replacement);
        }
        lastTimelinePosition.accept(electedTimeline.calculateNakadiLatestPosition(cursor.getPartition()));
        return result;
    }

    private NakadiCursor getBeforeFirstCursor(
            final TopicRepository topicRepository, final Timeline electedTimeline, final String partition)
            throws ServiceTemporarilyUnavailableException {
        final Optional<PartitionStatistics> statistics =
                topicRepository.loadPartitionStatistics(electedTimeline, partition);
        return statistics
                .orElseThrow(() -> new ServiceTemporarilyUnavailableException(
                        "It is expected that partition statistics exists for timeline " + electedTimeline +
                                " and partition " + partition + ", but it wasn't found")).getBeforeFirst();
    }

    private void electTopicRepositories() throws InvalidCursorException {
        final Map<TopicRepository, List<NakadiCursor>> newAssignment = new HashMap<>();
        borderOffsets.clear();
        // Purpose of this collection is to hold tr that definitely changed their positions and should be recreated.
        final Set<TopicPartition> actualReadPositionChanged = new HashSet<>();
        // load new topic repositories and possibly replace cursors to newer timelines.
        for (final NakadiCursor cursor : latestOffsets.values()) {
            final AtomicReference<NakadiCursor> cursorReplacement = new AtomicReference<>();
            final TopicRepository topicRepository = selectCorrectTopicRepo(
                    cursor,
                    cursorReplacement::set,
                    nc -> Optional.ofNullable(nc).ifPresent(
                            itm -> borderOffsets.put(itm.getEventTypePartition(), itm.getOffset())));
            if (!newAssignment.containsKey(topicRepository)) {
                newAssignment.put(topicRepository, new ArrayList<>());
            }
            if (cursorReplacement.get() != null) {
                actualReadPositionChanged.add(cursor.getTopicPartition());
                newAssignment.get(topicRepository).add(cursorReplacement.get());
            } else {
                newAssignment.get(topicRepository).add(cursor);
            }
        }
        final Set<TopicRepository> removedTopicRepositories = eventConsumers.keySet().stream()
                .filter(tr -> !newAssignment.containsKey(tr))
                .collect(Collectors.toSet());
        // Stop and remove event consumers that are not needed anymore
        for (final TopicRepository toRemove : removedTopicRepositories) {
            stopAndRemoveConsumer(toRemove);
        }
        // Stop and remove event consumers with changed configuration
        for (final Map.Entry<TopicRepository, List<NakadiCursor>> entry : newAssignment.entrySet()) {
            final EventConsumer.LowLevelConsumer existingEventConsumer = eventConsumers.get(entry.getKey());
            if (null != existingEventConsumer) {
                final Set<TopicPartition> newTopicPartitions = entry.getValue().stream()
                        .map(NakadiCursor::getTopicPartition)
                        .collect(Collectors.toSet());
                final Set<TopicPartition> oldAssignment = existingEventConsumer.getAssignment();
                if (!oldAssignment.equals(newTopicPartitions)
                        || oldAssignment.stream().anyMatch(actualReadPositionChanged::contains)) {
                    stopAndRemoveConsumer(entry.getKey());
                }
            }
        }
        // Start new consumers with changed configuration.
        for (final Map.Entry<TopicRepository, List<NakadiCursor>> entry : newAssignment.entrySet()) {
            if (!eventConsumers.containsKey(entry.getKey())) {
                final TopicRepository repo = entry.getKey();
                LOG.info("Creating underlying consumer for client id {} and cursors {}",
                        clientId, Arrays.deepToString(entry.getValue().toArray()));

                final EventConsumer.LowLevelConsumer consumer = repo.createEventConsumer(clientId, entry.getValue());
                eventConsumers.put(repo, consumer);
            }
        }
    }

    private void stopAndRemoveConsumer(final TopicRepository toRemove) {
        final EventConsumer realConsumer = eventConsumers.remove(toRemove);
        try {
            realConsumer.close();
        } catch (IOException ex) {
            LOG.error("Failed to stop one of consumers, but will not care about that, " +
                    "because it is already doesn't matter", ex);
        }
    }


    private void onTimelinesChanged() throws InvalidCursorException {
        final Set<String> eventTypes = latestOffsets.values().stream()
                .map(NakadiCursor::getEventType)
                .collect(Collectors.toSet());
        // Remove obsolete data
        final List<String> toRemove = eventTypeTimelines.keySet().stream()
                .filter(et -> !eventTypes.contains(et))
                .collect(Collectors.toList());
        for (final String item : toRemove) {
            eventTypeTimelines.remove(item);
        }
        // Add refreshed data.
        for (final String eventType : eventTypes) {
            final List<Timeline> newActiveTimelines = timelineService.getActiveTimelinesOrdered(eventType);
            eventTypeTimelines.put(eventType, newActiveTimelines);
        }
        // Second part - recreate TopicRepositories to fetch data from.
        electTopicRepositories();
    }

    @Override
    public void reassign(final Collection<NakadiCursor> newValues) throws InvalidCursorException {
        final Map<EventTypePartition, NakadiCursor> newCursorMap = newValues.stream()
                .collect(Collectors.toMap(NakadiCursor::getEventTypePartition, Function.identity()));

        final NakadiCollectionUtils.Diff<EventTypePartition> partitionsDiff = NakadiCollectionUtils.difference(
                latestOffsets.keySet(), newCursorMap.keySet());

        final NakadiCollectionUtils.Diff<String> eventTypesDiff = NakadiCollectionUtils.difference(
                timelineRefreshListeners.keySet(),
                newValues.stream().map(NakadiCursor::getEventType).collect(Collectors.toSet()));

        // first step - remove everything that is to be removed.
        cleanStreamedPartitions(partitionsDiff.getRemoved());
        // remove timeline sync listeners
        eventTypesDiff.getRemoved().stream()
                .map(timelineRefreshListeners::remove)
                .forEach(TimelineSync.ListenerRegistration::cancel);

        // remember new positions.
        partitionsDiff.getAdded().forEach(etp -> latestOffsets.put(etp, newCursorMap.get(etp)));
        // add new timeline listeners
        eventTypesDiff.getAdded().forEach(item ->
                timelineRefreshListeners.put(
                        item,
                        timelineSync.registerTimelineChangeListener(item, this::onTimelineChange)));

        // fetch all the timelines as a part of usual timeline update process
        onTimelinesChanged();
    }

    private void cleanStreamedPartitions(final Set<EventTypePartition> partitions) {
        partitions.forEach(latestOffsets::remove);
    }

    void onTimelineChange(final String eventType) {
        LOG.info("Received timeiline change notification for event type {}", eventType);
        timelinesChanged.set(true);
    }

    @Override
    public void close() throws IOException {
        try {
            reassign(Collections.emptySet());
        } catch (final InvalidCursorException e) {
            throw new IOException(e);
        }
    }
}
