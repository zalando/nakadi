package org.zalando.nakadi.service.job;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.exceptions.TopicDeletionException;
import org.zalando.nakadi.exceptions.runtime.InconsistentStateException;
import org.zalando.nakadi.exceptions.runtime.RepositoryProblemException;
import org.zalando.nakadi.repository.TopicRepository;
import org.zalando.nakadi.repository.db.EventTypeCache;
import org.zalando.nakadi.repository.db.TimelineDbRepository;
import org.zalando.nakadi.service.timeline.TimelineService;

@Service
public class TimelineCleanupJob {

    private static final String JOB_NAME = "timelines-cleanup";

    private static final Logger LOG = LoggerFactory.getLogger(TimelineCleanupJob.class);

    private final EventTypeCache eventTypeCache;
    private final TimelineDbRepository timelineDbRepository;
    private final TimelineService timelineService;
    private final ExclusiveJobWrapper jobWrapper;

    @Autowired
    public TimelineCleanupJob(final EventTypeCache eventTypeCache,
                              final TimelineDbRepository timelineDbRepository,
                              final TimelineService timelineService,
                              final JobWrapperFactory jobWrapperFactory,
                              @Value("${nakadi.jobs.timelineCleanup.runPeriodMs}") final int periodMs) {
        this.eventTypeCache = eventTypeCache;
        this.timelineDbRepository = timelineDbRepository;
        this.timelineService = timelineService;
        this.jobWrapper = jobWrapperFactory.createExclusiveJobWrapper(JOB_NAME, periodMs);
    }

    @Scheduled(
            fixedDelayString = "${nakadi.jobs.checkRunMs}",
            initialDelayString = "${random.int(${nakadi.jobs.checkRunMs})}")
    public void cleanupTimelines() {
        try {
            jobWrapper.runJobLocked(() ->
                    timelineDbRepository.getExpiredTimelines().stream()
                            .forEach(timeline -> {
                                deleteTimelineTopic(timeline);
                                markTimelineDeleted(timeline);
                            }));
        } catch (final RepositoryProblemException e) {
            LOG.error("DB error occurred when trying to get expired timelines", e);
        }
    }

    private void deleteTimelineTopic(final Timeline timeline) {
        try {
            final TopicRepository topicRepository = timelineService.getTopicRepository(timeline);
            topicRepository.deleteTopic(timeline.getTopic());
        } catch (final TopicDeletionException e) {
            LOG.error("Failed to delete topic {} for expired timeline {}", timeline.getTopic(), timeline.getId(), e);
        }
    }

    private void markTimelineDeleted(final Timeline timeline) {
        boolean timelineUpdatedInDB = false;
        boolean cacheUpdated = false;
        try {
            timeline.setDeleted(true);
            timelineDbRepository.updateTimelime(timeline);
            timelineUpdatedInDB = true;

            eventTypeCache.updated(timeline.getEventType());
            cacheUpdated = true;
        } catch (final InconsistentStateException e) {
            LOG.error("Failed to serialize timeline to DB when marking timeline as deleted", e);
        } catch (final RepositoryProblemException e) {
            LOG.error("DB failure when marking timeline as deleted", e);
        } catch (Exception e) {
            LOG.error("ZK error occurred when updating ET cache", e);
        } finally {
            // revert timeline state in a case if cache wasn't updated successfully
            if (timelineUpdatedInDB && !cacheUpdated) {
                try {
                    timeline.setDeleted(false);
                    timelineDbRepository.updateTimelime(timeline);
                } catch (final Exception e) {
                    LOG.error("Failed to revert timeline state", e);
                }
            }
        }
    }

}
