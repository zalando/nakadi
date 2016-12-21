package org.zalando.nakadi.service.timeline;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.Storage;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.exceptions.InternalNakadiException;
import org.zalando.nakadi.exceptions.NakadiException;
import org.zalando.nakadi.exceptions.NoSuchEventTypeException;
import org.zalando.nakadi.repository.EventTypeRepository;
import org.zalando.nakadi.repository.db.TimelineDbRepository;

import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

@Component
public class TimelineService {
    private final TimelineDbRepository timelineRepo;
    private final EventTypeRepository eventTypeRepo;
    private final TimelineSync timelineSync;

    @Autowired
    public TimelineService(
            final TimelineDbRepository timelineRepo,
            final EventTypeRepository eventTypeRepo,
            final TimelineSync timelineSync) {
        this.timelineRepo = timelineRepo;
        this.eventTypeRepo = eventTypeRepo;
        this.timelineSync = timelineSync;
    }

    public List<Storage> listStorages() {
        return timelineRepo.getStorages();
    }

    public Optional<Storage> getStorage(final String id) {
        return timelineRepo.getStorage(id);
    }

    public void createOrUpdateStorage(final Storage storage) throws NakadiException {
        final Optional<Storage> existing = timelineRepo.getStorage(storage.getId());
        if (existing.isPresent()) {
            if (!existing.get().getType().equals(storage.getType())) {
                throw new IllegalArgumentException("Can not change storage type for " + storage.getId());
            }
            final List<Timeline> timelines =
                    timelineRepo.listTimelines(null, storage.getId(), null, null);
            if (!timelines.isEmpty()) {
                throw new IllegalStateException("Timelines are present for storage " + storage.getId());
            }
            timelineRepo.update(storage);
        } else {
            timelineRepo.create(storage);
        }
    }

    public void deleteStorage(final String storageId) {
        timelineRepo.getStorage(storageId).ifPresent(storage -> {
            final List<Timeline> existingTimelines =
                    timelineRepo.listTimelines(null, storage.getId(), null, null);
            if (!existingTimelines.isEmpty()) {
                throw new IllegalStateException("Storage " + storage.getId() + " have linked timelines. Can not delete");
            }
            timelineRepo.delete(storage);
        });
    }

    public List<Timeline> listTimelines(final String eventType) {
        return timelineRepo.listTimelines(eventType, null, null, null);
    }

    public Timeline createAndStartTimeline(final String eventTypeName, final String storageId) throws InternalNakadiException, NoSuchEventTypeException, InterruptedException {
        final Storage storage = timelineRepo.getStorage(storageId)
                .orElseThrow(() -> new IllegalArgumentException("Storage with id " + storageId + " is not found"));
        final EventType eventType = eventTypeRepo.findByName(eventTypeName);
        final Timeline activeTimeline = timelineRepo.loadActiveTimeline(eventTypeName);

        final Timeline timeline = new Timeline();
        timeline.setCreatedAt(new Date());
        timeline.setEventType(eventTypeName);
        timeline.setStorage(storage);

        final StorageWorker storageWorker = StorageWorker.build(storage);

        if (null == activeTimeline) {
            // Here everything is simple.
            // Nothing will be changed - topic will remain the same, but it will start to work with different offsets
            // format.
            // We don't have to stop publishers, but we will, because it will reduce complexity of update process.
            if (!storage.equals(timelineRepo.getDefaultStorage())) {
                throw new IllegalArgumentException("The very first change should go to default storage");
            }
            timeline.setStorageConfiguration(storageWorker.createEventTypeConfiguration(eventType, true));
            timeline.setOrder(0);
        } else {
            timeline.setStorageConfiguration(storageWorker.createEventTypeConfiguration(eventType, false));
            timeline.setOrder(activeTimeline.getOrder() + 1);
        }

        // Now perform actual switch (try to do it within 60 seconds).
        timelineSync.startTimelineUpdate(eventType.getName(), TimeUnit.SECONDS.toMillis(60));
        try {
            // Now all nodes have stopped publishing to event type, they are waiting for unblock.
            timeline.setSwitchedAt(new Date());
            if (null != activeTimeline) {
                // Wee need to write latest cursors configuration.
                final StorageWorker oldStorage = StorageWorker.build(activeTimeline.getStorage());
                activeTimeline.setLastPosition(oldStorage.getLatestPosition(activeTimeline));
                timelineRepo.update(activeTimeline);
            }
            return timelineRepo.create(timeline);
        } finally {
            // Inform everyone that it's time to reread information.
            timelineSync.finishTimelineUpdate(eventType.getName());
        }
    }


    public void deleteTimeline(final String eventType, final Integer timelineId) throws InterruptedException {
        // The only possible way to delete timeline - delete active timeline that has order 0 and is linked to default
        // storage.
        final Timeline timeline = timelineRepo.getTimeline(timelineId);
        if (null == timeline) {
            throw new IllegalArgumentException("Timeline with id " + timelineId + " is not found");
        }
        if (!timeline.getEventType().equals(eventType)) {
            throw new IllegalArgumentException("Timeline event type: " + timeline.getEventType() + ", provided: " + eventType);
        }
        if (timeline.getOrder() != 0) {
            throw new IllegalStateException("Can remove only the very first timeline");
        }
        if (!Objects.equals(timeline.getId(), timelineRepo.loadActiveTimeline(eventType).getId())) {
            throw new IllegalStateException("Timeline is not the latest one!");
        }
        timelineSync.startTimelineUpdate(timeline.getEventType(), TimeUnit.SECONDS.toMillis(60));
        try {
            timelineRepo.delete(timeline);
        } finally {
            timelineSync.finishTimelineUpdate(timeline.getEventType());
        }
    }
}
