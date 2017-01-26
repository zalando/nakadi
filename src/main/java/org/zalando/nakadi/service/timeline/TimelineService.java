package org.zalando.nakadi.service.timeline;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.repository.db.TimelineDbRepository;

import java.util.List;

@Service
public class TimelineService {
    private final TimelineDbRepository timelineDbRepository;

    @Autowired
    public TimelineService(final TimelineDbRepository timelineDbRepository) {
        this.timelineDbRepository = timelineDbRepository;
    }

    public List<Timeline> listTimelines() {
        return timelineDbRepository.listTimelines();
    }
}
