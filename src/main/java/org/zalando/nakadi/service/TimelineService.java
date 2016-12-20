package org.zalando.nakadi.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.domain.Storage;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.exceptions.NakadiException;
import org.zalando.nakadi.repository.db.TimelineDbRepository;

import java.util.List;
import java.util.Optional;

@Component
public class TimelineService {
    private final TimelineDbRepository timelineDbRepository;

    @Autowired
    public TimelineService(final TimelineDbRepository timelineDbRepository) {
        this.timelineDbRepository = timelineDbRepository;
    }

    public List<Storage> listStorages() {
        return timelineDbRepository.getStorages();
    }

    public Optional<Storage> getStorage(final String id) {
        return timelineDbRepository.getStorage(id);
    }

    public void createOrUpdateStorage(final Storage storage) throws NakadiException {
        final Optional<Storage> existing = timelineDbRepository.getStorage(storage.getId());
        if (existing.isPresent()) {
            if (!existing.get().getType().equals(storage.getType())) {
                throw new IllegalArgumentException("Can not change storage type for " + storage.getId());
            }
            final List<Timeline> timelines =
                    timelineDbRepository.listTimelines(null, storage.getId(), null, null);
            if (!timelines.isEmpty()) {
                throw new IllegalStateException("Timelines are present for storage " + storage.getId());
            }
            timelineDbRepository.update(storage);
        } else {
            timelineDbRepository.create(storage);
        }
    }

    public void deleteStorage(final String storageId) {
        timelineDbRepository.getStorage(storageId).ifPresent(storage -> {
            final List<Timeline> existingTimelines =
                    timelineDbRepository.listTimelines(null, storage.getId(), null, null);
            if (!existingTimelines.isEmpty()) {
                throw new IllegalStateException("Storage " + storage.getId() + " have linked timelines. Can not delete");
            }
            timelineDbRepository.delete(storage);
        });
    }
}
