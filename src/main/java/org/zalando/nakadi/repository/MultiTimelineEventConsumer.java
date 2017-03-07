package org.zalando.nakadi.repository;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zalando.nakadi.domain.ConsumedEvent;
import org.zalando.nakadi.domain.EventTypePartition;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.PartitionStatistics;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.domain.TopicPartition;
import org.zalando.nakadi.exceptions.InvalidCursorException;
import org.zalando.nakadi.exceptions.NakadiException;
import org.zalando.nakadi.exceptions.NakadiRuntimeException;
import org.zalando.nakadi.exceptions.ServiceUnavailableException;
import org.zalando.nakadi.service.timeline.TimelineService;
import org.zalando.nakadi.service.timeline.TimelineSync;
import org.zalando.nakadi.util.NakadiUtils;

public class MultiTimelineEventConsumer implements EventConsumer {
    private final String clientId;
    private final Map<EventTypePartition, NakadiCursor> latestOffsets = new HashMap<>();
    private final Map<String, List<Timeline>> consumedEventTypes = new HashMap<>();
    private final Map<String, TimelineSync.ListenerRegistration> timelineRefreshListeners = new HashMap<>();
    private final Map<TopicRepository, EventConsumer> eventConsumers = new HashMap<>();
    private final LinkedList<ConsumedEvent> eventsQueue = new LinkedList<>();
    private final Map<EventTypePartition, String> borderOffsets = new HashMap<>();
    private final TimelineService timelineService;
    private final TimelineSync timelineSync;
    private final BlockingQueue<Runnable> queuedTasks = new LinkedBlockingQueue<>();
    private static final Logger LOG = LoggerFactory.getLogger(MultiTimelineEventConsumer.class);

    public MultiTimelineEventConsumer(
            final String clientId,
            final TimelineService timelineService,
            final TimelineSync timelineSync) {
        this.clientId = clientId;
        this.timelineService = timelineService;
        this.timelineSync = timelineSync;
    }

    @Override
    public Set<TopicPartition> getAssignment() {
        return latestOffsets.values().stream().map(NakadiCursor::getTopicPartition).collect(Collectors.toSet());
    }


    @Override
    public Optional<ConsumedEvent> readEvent() {
        Runnable task;
        while ((task = queuedTasks.poll()) != null) {
            task.run();
        }
        final Optional<ConsumedEvent> result = Optional.ofNullable(eventsQueue.poll());
        if (result.isPresent()) {
            final NakadiCursor position = result.get().getPosition();
            final EventTypePartition etp = position.getEventTypePartition();
            latestOffsets.put(etp, position);
            final String borderOffset = borderOffsets.get(etp);
            if (null != borderOffset && borderOffset.compareTo(position.getOffset()) <= 0) {
                queuedTasks.add(() -> {
                    try {
                        reelectTopicRepositories();
                    } catch (final NakadiException | InvalidCursorException e) {
                        throw new NakadiRuntimeException(e);
                    }
                });
            }
        } else {
            poll();
        }
        return result;
    }

    private void poll() {
        eventConsumers.values().forEach(consumer -> {
            consumer.readEvent().ifPresent(eventsQueue::add);
        });
    }

    private TopicRepository selectCorrectTopicRepo(
            final NakadiCursor cursor,
            final Consumer<NakadiCursor> cursorReplacer,
            final Consumer<NakadiCursor> lastTimelinePosition)
            throws ServiceUnavailableException {
        final List<Timeline> eventTimelines = consumedEventTypes.get(cursor.getEventType());
        Timeline electedTimeline = null;
        final ListIterator<Timeline> itTimeline = eventTimelines.listIterator(eventTimelines.size());
        while (itTimeline.hasPrevious()) {
            final Timeline toCheck = itTimeline.previous();
            final NakadiCursor latest = toCheck.calculateNakadiLatestPosition(cursor.getPartition());
            if (latest == null || latest.getOffset().compareTo(cursor.getOffset()) > 0) {
                electedTimeline = toCheck;
            } else {
                break;
            }
        }
        Preconditions.checkNotNull(electedTimeline);
        final TopicRepository result = timelineService.getTopicRepository(electedTimeline);
        if (electedTimeline.getOrder() > cursor.getTimeline().getOrder()) {
            // It seems that cursor jumped to different timeline. One need to fetch very first cursor in timeline.
            cursorReplacer.accept(getBeforeFirstCursor(result, electedTimeline, cursor.getPartition()));
        }
        if (electedTimeline.getLatestPosition() != null) {
            electedTimeline.calculateNakadiLatestPosition(cursor.getPartition());
        } else {
            lastTimelinePosition.accept(null);
        }
        return result;
    }

    private NakadiCursor getBeforeFirstCursor(
            final TopicRepository topicRepository, final Timeline electedTimeline, final String partition)
            throws ServiceUnavailableException {
        final Optional<PartitionStatistics> statistics =
                topicRepository.loadPartitionStatistics(electedTimeline, partition);
        return statistics
                .orElseThrow(() -> new ServiceUnavailableException(
                        "It is expected that partition statistics exists for timeline " + electedTimeline +
                                " and partition " + partition + ", but it wasn't found")).getBeforeFirst();
    }

    private void reelectTopicRepositories() throws NakadiException, InvalidCursorException {
        final Map<TopicRepository, List<NakadiCursor>> newAssignment = new HashMap<>();
        borderOffsets.clear();
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
            newAssignment.get(topicRepository).add(Optional.ofNullable(cursorReplacement.get()).orElse(cursor));
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
            final EventConsumer existingEventConsumer = eventConsumers.get(entry.getKey());
            if (null != existingEventConsumer) {
                final Set<TopicPartition> newTopicPartitions = entry.getValue().stream()
                        .map(NakadiCursor::getTopicPartition)
                        .collect(Collectors.toSet());
                if (!existingEventConsumer.getAssignment().equals(newTopicPartitions)) {
                    stopAndRemoveConsumer(entry.getKey());
                }
            }
        }
        // Start new consumers with changed configuration.
        for (final Map.Entry<TopicRepository, List<NakadiCursor>> entry : newAssignment.entrySet()) {
            if (!eventConsumers.containsKey(entry.getKey())) {
                final TopicRepository repo = entry.getKey();
                final EventConsumer consumer = repo.createEventConsumer(clientId, entry.getValue());
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


    private void timelinesChanged() throws NakadiException, InvalidCursorException {
        final Set<String> eventTypes = latestOffsets.values().stream()
                .map(NakadiCursor::getEventType)
                .collect(Collectors.toSet());
        // Remove obsolete data
        final List<String> toRemove = consumedEventTypes.keySet().stream()
                .filter(et -> !eventTypes.contains(et))
                .collect(Collectors.toList());
        for (final String item : toRemove) {
            consumedEventTypes.remove(item);
        }
        // Add refreshed data.
        for (final String eventType : eventTypes) {
            consumedEventTypes.put(eventType, timelineService.getActiveTimelinesOrdered(eventType));
        }
        // Second part - recreate TopicRepositories to fetch data from.
        reelectTopicRepositories();
    }


    public void reassign(final Collection<NakadiCursor> newValues) throws NakadiException, InvalidCursorException {
        final Map<EventTypePartition, NakadiCursor> newCursorMap = newValues.stream()
                .collect(Collectors.toMap(NakadiCursor::getEventTypePartition, Function.identity()));

        final NakadiUtils.Diff<EventTypePartition> partitionsDiff = NakadiUtils.difference(
                latestOffsets.keySet(), newCursorMap.keySet());

        final NakadiUtils.Diff<String> eventTypesDiff = NakadiUtils.difference(
                timelineRefreshListeners.keySet(),
                newValues.stream().map(NakadiCursor::getEventType).collect(Collectors.toSet()));

        // first step - remove everything that is to be removed.
        cleanStreamedPartitions(partitionsDiff.removed);
        // remove timeline sync listeners
        eventTypesDiff.removed.stream()
                .map(timelineRefreshListeners::remove)
                .forEach(TimelineSync.ListenerRegistration::cancel);

        // remember new positions.
        partitionsDiff.added.forEach(etp -> {
            latestOffsets.put(etp, newCursorMap.get(etp));
        });
        // add new timeline listeners
        eventTypesDiff.added.forEach(item ->
                timelineRefreshListeners.put(
                        item,
                        timelineSync.registerTimelineChangeListener(item, this::onTimelineChange)));

        // fetch all the timelines as a part of usual timeline update process
        timelinesChanged();
    }

    private void cleanStreamedPartitions(final Set<EventTypePartition> partitions) {
        eventsQueue.removeIf(event -> partitions.contains(event.getPosition().getEventTypePartition()));
        partitions.forEach(latestOffsets::remove);
    }

    private void onTimelineChange(final String ignore) {
        queuedTasks.add(() -> {
            try {
                timelinesChanged();
            } catch (final NakadiException | InvalidCursorException ex) {
                throw new NakadiRuntimeException(ex);
            }
        });
    }

    @Override
    public void close() throws IOException {
        try {
            reassign(Collections.emptySet());
        } catch (final NakadiException | InvalidCursorException e) {
            throw new IOException(e);
        }
    }
}
